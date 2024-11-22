use crate::anchor_lang::{AnchorDeserialize, Discriminator};
use log::error;

use super::EventListenerError;

struct Execution<'a> {
    program_stack: Vec<&'a str>,
}

impl<'a> Execution<'a> {
    fn program(&self) -> Result<&'a str, EventListenerError> {
        self.program_stack.last().copied().ok_or_else(|| {
            error!("Failed to get program from the empty stack");
            EventListenerError::SolanaParseLogs
        })
    }

    fn push(&mut self, new_program: &'a str) {
        self.program_stack.push(new_program);
    }

    fn pop(&mut self) -> Result<(), EventListenerError> {
        self.program_stack
            .pop()
            .ok_or_else(|| {
                error!("Failed to get program from the empty stack");
                EventListenerError::SolanaParseLogs
            })
            .map(|_| ())
    }

    fn update(&mut self, log: &'a str) -> Result<(), EventListenerError> {
        const PROGRAM_START: &str = "Program ";
        const INVOKE: &str = " invoke";

        if !log.starts_with(PROGRAM_START) {
            return Ok(());
        }

        let Some(space_pos) = log.as_bytes()[PROGRAM_START.len()..]
            .iter()
            .position(|&b| b == b' ')
            .map(|pos| PROGRAM_START.len() + pos)
        else {
            return Ok(());
        };

        if !log.as_bytes()[space_pos..].starts_with(INVOKE.as_bytes()) {
            return Ok(());
        }

        let program = &log[PROGRAM_START.len()..space_pos];
        if program.contains(':') {
            return Ok(());
        }

        self.push(program);
        Ok(())
    }
}

pub(crate) fn parse_logs<T: AnchorDeserialize + Discriminator>(
    logs: &[&str],
    program_id_str: &str,
) -> Result<Vec<T>, EventListenerError> {
    let mut events = Vec::new();
    let mut do_pop = false;
    if !logs.is_empty() {
        let mut execution = Execution {
            program_stack: Vec::with_capacity(4),
        };
        for log in logs {
            if do_pop {
                execution.pop()?;
            }
            execution.update(log)?;
            let (event, pop) = if program_id_str == execution.program()? {
                handle_program_log(log).map_err(|e| {
                    error!("Failed to parse log: {}", e);
                    EventListenerError::SolanaParseLogs
                })?
            } else {
                (None, is_program_end(log))
            };
            do_pop = pop;
            if let Some(e) = event {
                events.push(e);
            }
        }
    }
    Ok(events)
}

fn handle_program_log<T: AnchorDeserialize + Discriminator>(
    l: &str,
) -> Result<(Option<T>, bool), EventListenerError> {
    const PROGRAM_DATA: &str = "Program data: ";

    if let Some(log) = l.strip_prefix(PROGRAM_DATA) {
        #[allow(deprecated)]
        let Ok(borsh_bytes) = crate::anchor_lang::__private::base64::decode(log) else {
            return Ok((None, false));
        };

        if borsh_bytes.get(..8) != Some(&T::discriminator()) {
            return Ok((None, false));
        };

        let event = Some(T::deserialize(&mut &borsh_bytes[8..]).map_err(|err| {
            error!("Failed to deserialize event: {}", err);
            EventListenerError::SolanaParseLogs
        })?);
        Ok((event, false))
    } else {
        Ok((None, is_program_end(l)))
    }
}

fn is_program_end(log: &str) -> bool {
    const PROGRAM_START: &str = "Program ";
    const SUCCESS: &str = " success";
    log.starts_with(PROGRAM_START)
        && log.ends_with(SUCCESS)
        && log.as_bytes()[PROGRAM_START.len()..log.len() - SUCCESS.len()]
            .iter()
            .all(|&b| b != b' ' && b != b':')
}

#[cfg(test)]
mod test {
    use crate::anchor_lang::{self, prelude::*};
    use crate::solana_logs::parse_logs;

    #[event]
    pub struct ProposeEvent {
        pub protocol_id: Vec<u8>,
        pub nonce: u64,
        pub dst_chain_id: u128,
        pub protocol_address: Vec<u8>,
        pub function_selector: Vec<u8>,
        pub params: Vec<u8>,
    }

    #[test]
    fn test_logs_parsing() {
        static SAMPLE: &[&str] = &[
            "Program EjpcUpcuJV2Mq9vjELMZHhgpvJ4ggoWtUYCTFqw6D9CZ invoke [1]",
            "Program log: Instruction: ShareMessage",
            "Program log: Share message invoked",
            "Program 3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ invoke [2]",
            "Program log: Instruction: Propose",
            "Program data: 8vb9LnW1kqUgAAAAb25lZnVuY19fX19fX19fX19fX19fX19fX19fX19fX18IAAAAAAAAAG2BAAAAAAAAAAAAAAAAAAADAAAAAQIDAwAAAAECAwMAAAABAgM=",
            "Program 3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ consumed 16408 of 181429 compute units",
            "Program 3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ success",
            "Program EjpcUpcuJV2Mq9vjELMZHhgpvJ4ggoWtUYCTFqw6D9CZ consumed 35308 of 200000 compute units",
            "Program EjpcUpcuJV2Mq9vjELMZHhgpvJ4ggoWtUYCTFqw6D9CZ success",
        ];

        let events: Vec<ProposeEvent> =
            parse_logs::parse_logs(SAMPLE, "3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ")
                .expect("Processing logs should not result in errors");
        assert_eq!(events.len(), 1);
        let propose_event = events.first().expect("No events caught");
        assert_eq!(propose_event.dst_chain_id, 33133);
        assert_eq!(propose_event.params, vec![1, 2, 3]);
        assert_eq!(
            propose_event.protocol_id.as_slice(),
            b"onefunc_________________________"
        );
    }

    #[test]
    fn test_logs_parsing_ignores_injection() {
        static SAMPLE: &[&str] = &[
            "Program EjpcUpcuJV2Mq9vjELMZHhgpvJ4ggoWtUYCTFqw6D9CZ invoke [1]",
            "Program log: I am going to invoke",
            "Program log: I strive for success",
            "Program log: I strive for success",
            "Program EjpcUpcuJV2Mq9vjELMZHhgpvJ4ggoWtUYCTFqw6D9CZ success",
            "Program EjpcUpcuJV2Mq9vjELMZHhgpvJ4ggoWtUYCTFqw6D9CZ invoke [1]",
            "Program log: Instruction: ShareMessage",
            "Program log: Share message invoked",
            "Program 3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ invoke [2]",
            "Program log: Instruction: Propose",
            "Program data: 8vb9LnW1kqUgAAAAb25lZnVuY19fX19fX19fX19fX19fX19fX19fX19fX18IAAAAAAAAAG2BAAAAAAAAAAAAAAAAAAADAAAAAQIDAwAAAAECAwMAAAABAgM=",
            "Program 3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ consumed 16408 of 181429 compute units",
            "Program 3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ success",
            "Program EjpcUpcuJV2Mq9vjELMZHhgpvJ4ggoWtUYCTFqw6D9CZ consumed 35308 of 200000 compute units",
            "Program EjpcUpcuJV2Mq9vjELMZHhgpvJ4ggoWtUYCTFqw6D9CZ success",
        ];

        let events: Vec<ProposeEvent> =
            parse_logs::parse_logs(SAMPLE, "3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ")
                .expect("Processing logs should not result in errors");
        assert_eq!(events.len(), 1);
        let propose_event = events.first().expect("No events caught");
        assert_eq!(propose_event.dst_chain_id, 33133);
        assert_eq!(propose_event.params, vec![1, 2, 3]);
        assert_eq!(
            propose_event.protocol_id.as_slice(),
            b"onefunc_________________________"
        );
    }

    #[test]
    fn test_deploy_programs() {
        static SAMPLE: &[&str] = &[
            "Program 11111111111111111111111111111111 invoke [1]",
            "Program 11111111111111111111111111111111 success",
            "Program BPFLoaderUpgradeab1e11111111111111111111111 invoke [1]",
            "Program 11111111111111111111111111111111 invoke [2]",
            "Program 11111111111111111111111111111111 success",
            "Deployed program 3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ",
            "Program BPFLoaderUpgradeab1e11111111111111111111111 success",
        ];
        let events: Vec<ProposeEvent> =
            parse_logs::parse_logs(SAMPLE, "3cAFEXstVzff2dXH8PFMgm81h8sQgpdskFGZqqoDgQkJ")
                .expect("Processing logs should not result in errors");
        assert!(events.is_empty(), "Expected no events have been met")
    }
}
