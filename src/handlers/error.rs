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
    ObjectNotKnown,
    ObjectNotValid,
    ObjectIdWrong,
    MessageNotValid,
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
            Self::ObjectNotKnown => (StatusCode::NOT_FOUND, "object is not known"),
            Self::ObjectNotValid => (StatusCode::BAD_REQUEST, "object is not valid"),
            Self::ObjectIdWrong => (StatusCode::BAD_REQUEST, "object ID is wrong"),
            Self::MessageNotValid => (StatusCode::BAD_REQUEST, "message is not valid"),
        }
        .into_response()
    }
}
