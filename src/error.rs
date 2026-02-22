use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}
