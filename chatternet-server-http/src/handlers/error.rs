use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug)]
pub enum AppError {
    DbConnectionFailed,
    DbQueryFailed,
    DidNotValid,
    ActorNotKnown,
    ActorNotValid,
    ActorIdWrong,
    DocumentNotKnown,
    DocumentNotValid,
    DocumentIdWrong,
    MessageNotValid,
    ServerMisconfigured,
    StaleMessage,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            Self::DbConnectionFailed => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "database connection failed",
            ),
            Self::DbQueryFailed => (StatusCode::INTERNAL_SERVER_ERROR, "database query failed"),
            Self::DidNotValid => (StatusCode::BAD_REQUEST, "DID is not valid"),
            Self::ActorNotKnown => (StatusCode::NOT_FOUND, "actor is not known"),
            Self::ActorNotValid => (StatusCode::BAD_REQUEST, "actor is not valid"),
            Self::ActorIdWrong => (StatusCode::BAD_REQUEST, "actor ID is wrong"),
            Self::DocumentNotKnown => (StatusCode::NOT_FOUND, "document is not known"),
            Self::DocumentNotValid => (StatusCode::BAD_REQUEST, "document is not valid"),
            Self::DocumentIdWrong => (StatusCode::BAD_REQUEST, "document ID is wrong"),
            Self::MessageNotValid => (StatusCode::BAD_REQUEST, "message is not valid"),
            Self::ServerMisconfigured => {
                (StatusCode::INTERNAL_SERVER_ERROR, "server is misconfigured")
            }
            Self::StaleMessage => (
                StatusCode::CONFLICT,
                "a newer timestamp is known for this object",
            ),
        }
        .into_response()
    }
}
