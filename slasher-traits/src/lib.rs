pub use self::validator::{
    BlockValidationScope, NoopValidatorEventsRecorder, ReceivedSignature, ValidationSessionId,
    ValidatorEvents, ValidatorEventsListener, ValidatorSessionScope,
};

mod validator;

pub type PeerIdInner = [u8; 32];
