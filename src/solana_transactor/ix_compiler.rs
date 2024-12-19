use solana_sdk::{
    address_lookup_table_account::AddressLookupTableAccount,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::{v0::Message, VersionedMessage},
    pubkey::Pubkey,
};
use std::fmt::Display;

use super::{ TransactorError};
use crate::log_with_ctx;

const DEFAULT_CU: u32 = 200_000;
const MAX_CU: u32 = 1_400_000;
const MAX_MSG_LEN: usize = 1232 - 65; // assuming only one signature

#[derive(Debug, Clone)]
pub struct InstructionBundle {
    pub instruction: Instruction,
    pub compute_units: u32,
}

impl InstructionBundle {
    pub fn new(instruction: Instruction, compute_units: u32) -> Self {
        Self {
            instruction,
            compute_units,
        }
    }
}

pub struct IxCompiler {
    ix_buffer: Vec<Instruction>,
    total_compute_units: u32,
    payer: Pubkey,
    address_lookup_table_accounts: Vec<AddressLookupTableAccount>,
    compute_units_price: Option<u64>,
}

impl IxCompiler {
    pub fn new(payer: Pubkey, compute_units_price: Option<u64>) -> Self {
        Self {
            ix_buffer: Vec::new(),
            total_compute_units: 0,
            payer,
            address_lookup_table_accounts: Vec::new(),
            compute_units_price,
        }
    }

    pub fn set_cu_price(&mut self, compute_units_price: Option<u64>) {
        self.compute_units_price = compute_units_price;
    }

    pub fn get_ix_price_if_any(&self) -> Vec<Instruction> {
        self.compute_units_price
            .map(ComputeBudgetInstruction::set_compute_unit_price)
            .into_iter()
            .collect()
    }

    /// Try to pack buffered instructions into message, return next message to send if close to or over transaction size limit
    pub fn compile<T: Display>(
        &mut self,
        log_ctx: Option<T>,
        ix: Instruction,
        address_lookup_table_accounts: &[AddressLookupTableAccount],
        compute_units: u32,
    ) -> Result<Option<VersionedMessage>, TransactorError> {
        // Initial instruction validation
        let msg = Message::try_compile(
            &self.payer,
            &[
                &get_compute_units_ix_if_any(compute_units),
                &self.get_ix_price_if_any()[..],
                &[ix.clone()],
            ]
            .concat(),
            address_lookup_table_accounts,
            Hash::default(),
        )?;
        let msg = VersionedMessage::V0(msg);
        let msg_len = msg.serialize().len();
        if exceeds_limits(msg_len, compute_units) {
            return Err(TransactorError::InstructionTooBig);
        }

        let total_compute_units = self.total_compute_units + compute_units;
        let ix_buffer = [
            &get_compute_units_ix_if_any(total_compute_units),
            &self.get_ix_price_if_any()[..],
            &self.ix_buffer[..],
            &[ix.clone()],
        ]
        .concat();
        let address_lookup_table_accounts_all = [
            &self.address_lookup_table_accounts[..],
            address_lookup_table_accounts,
        ]
        .concat();
        let msg = Message::try_compile(
            &self.payer,
            &ix_buffer,
            &address_lookup_table_accounts_all,
            Hash::default(),
        )?;
        let msg = VersionedMessage::V0(msg);
        let msg_len = msg.serialize().len();
        log_with_ctx!(
            debug,
            log_ctx,
            "Instructions: {} Tx len: {} CU: {}",
            self.ix_buffer.len(),
            msg_len,
            total_compute_units
        );
        if exceeds_limits(msg_len, total_compute_units) {
            log_with_ctx!(debug, log_ctx, "Tx limit reached, sending previous instructions...");
            let msg = Message::try_compile(
                &self.payer,
                &[
                    &get_compute_units_ix_if_any(self.total_compute_units),
                    &self.get_ix_price_if_any()[..],
                    &self.ix_buffer[..],
                ]
                .concat(),
                &self.address_lookup_table_accounts,
                Hash::default(),
            )?;
            self.ix_buffer.clear();
            self.ix_buffer.push(ix);
            self.address_lookup_table_accounts.clear();
            self.address_lookup_table_accounts.extend_from_slice(address_lookup_table_accounts);
            self.total_compute_units = compute_units;
            return Ok(Some(VersionedMessage::V0(msg)));
        } else if approaches_limits(msg_len, total_compute_units) {
            log_with_ctx!(debug, log_ctx, "Tx limit reached, sending current instructions...");
            self.ix_buffer.clear();
            self.address_lookup_table_accounts.clear();
            self.total_compute_units = 0;
            return Ok(Some(msg));
        }
        self.ix_buffer.push(ix);
        self.address_lookup_table_accounts.extend_from_slice(address_lookup_table_accounts);
        self.total_compute_units = total_compute_units;
        Ok(None)
    }

    pub fn flush(&mut self) -> Result<Option<VersionedMessage>, TransactorError> {
        if self.ix_buffer.is_empty() {
            return Ok(None);
        }
        let msg = Message::try_compile(
            &self.payer,
            &[
                &get_compute_units_ix_if_any(self.total_compute_units),
                &self.get_ix_price_if_any()[..],
                &self.ix_buffer[..],
            ]
            .concat(),
            &self.address_lookup_table_accounts,
            Hash::default(),
        )?;
        self.ix_buffer.clear();
        self.address_lookup_table_accounts.clear();
        self.total_compute_units = 0;
        Ok(Some(VersionedMessage::V0(msg)))
    }
}

/// Returns true if tx exceeds limits
fn exceeds_limits(msg_len: usize, compute_units: u32) -> bool {
    msg_len > MAX_MSG_LEN || compute_units > MAX_CU
}

/// Returns true if tx approaches limits
fn approaches_limits(msg_len: usize, compute_units: u32) -> bool {
    msg_len >= MAX_MSG_LEN - 32 || compute_units >= MAX_CU - 200_000
}

fn get_compute_units_ix_if_any(compute_units: u32) -> Vec<Instruction> {
    if compute_units <= DEFAULT_CU {
        return Vec::new();
    }
    vec![ComputeBudgetInstruction::set_compute_unit_limit(
        compute_units,
    )]
}

#[cfg(test)]
mod test {
    use solana_sdk::{
        instruction::AccountMeta, signature::Keypair, signer::Signer,
        transaction::VersionedTransaction,
    };

    use super::*;

    #[test]
    fn test_ix_compile() {
        env_logger::Builder::new().filter_level(log::LevelFilter::Debug).init();
        let signer = Keypair::new();
        let program = Keypair::new();
        let accounts = [
            &vec![AccountMeta::new(Keypair::new().pubkey(), false); 9][..],
            &[AccountMeta::new(signer.pubkey(), true)],
        ]
        .concat();
        let ix = Instruction::new_with_bytes(program.pubkey(), &[1; 128], accounts);
        let mut ix_compiler = IxCompiler::new(signer.pubkey(), Some(1000));
        let mut n = 0;
        let msg = loop {
            if let Some(msg) = ix_compiler.compile::<&str>(None, ix.clone(), &[], 20000).unwrap() {
                break msg;
            } else {
                n += 1;
            }
        };
        let tx = VersionedTransaction::try_new(msg, &[&signer]).unwrap();
        let tx_raw: Vec<u8> = bincode::serialize(&tx).unwrap();
        println!("Packed {} ix", n);
        println!("Tx len {}", tx_raw.len());
        assert!(tx_raw.len() <= 1232);
        let flush_msg = ix_compiler.flush().unwrap().unwrap();
        let tx = VersionedTransaction::try_new(flush_msg, &[&signer]).unwrap();
        let tx_raw: Vec<u8> = bincode::serialize(&tx).unwrap();
        println!("Flush tx len {}", tx_raw.len());
        assert!(tx_raw.len() <= 1232);

        let msg = ix_compiler.compile::<&str>(None, ix.clone(), &[], 1200000).unwrap().unwrap();
        let tx = VersionedTransaction::try_new(msg, &[&signer]).unwrap();
        let tx_raw: Vec<u8> = bincode::serialize(&tx).unwrap();
        assert!(tx_raw.len() <= 1232);
    }
}
