use warp::reject::Reject;

#[derive(Debug)]
pub enum Error {
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
    IdWrong,
}
impl Reject for Error {}
