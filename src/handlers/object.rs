use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::http::StatusCode;
use warp::Rejection;

use super::error::Error;
use crate::chatternet::activities::{Message, Object};
use crate::db::{self, Connector};

pub async fn handle_object_get(
    object_id: String,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    // read only
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| Error::DbConnectionFailed)?;

    if db::has_object(&mut connection, &object_id)
        .await
        .map_err(|_| Error::DbQueryFailed)?
    {
        let object = db::get_object(&mut connection, &object_id)
            .await
            .map_err(|_| Error::DbQueryFailed)?;
        match object {
            Some(object) => {
                let object: Object =
                    serde_json::from_str(&object).map_err(|_| Error::ObjectNotValid)?;
                return Ok(warp::reply::json(&object));
            }
            None => {
                return Ok(warp::reply::json(&serde_json::Value::Null));
            }
        }
    } else if db::has_message(&mut connection, &object_id)
        .await
        .map_err(|_| Error::DbQueryFailed)?
    {
        let object = db::get_message(&mut connection, &object_id)
            .await
            .map_err(|_| Error::DbQueryFailed)?;
        match object {
            Some(object) => {
                let object: Message =
                    serde_json::from_str(&object).map_err(|_| Error::MessageNotValid)?;
                return Ok(warp::reply::json(&object));
            }
            None => {
                return Ok(warp::reply::json(&serde_json::Value::Null));
            }
        }
    }

    Err(Error::ObjectNotKnown)?
}

pub async fn handle_object_post(
    object_id: String,
    object: Object,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    // read write
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| Error::DbConnectionFailed)?;
    if object_id.is_empty()
        || object
            .id
            .as_ref()
            .map(|x| x.as_str() != object_id)
            .unwrap_or(true)
    {
        Err(Error::ObjectIdWrong)?;
    }
    if !db::has_object(&mut *connection, &object_id)
        .await
        .map_err(|_| Error::DbQueryFailed)?
    {
        Err(Error::ObjectNotKnown)?;
    }
    if !object.verify().await.is_ok() {
        Err(Error::ObjectNotValid)?;
    }
    let object = serde_json::to_string(&object).map_err(|_| Error::ObjectNotValid)?;
    db::put_or_update_object(&mut *connection, &object_id, Some(&object))
        .await
        .map_err(|_| Error::DbQueryFailed)?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use tokio;
    use warp::{http::StatusCode, test::request};

    use crate::chatternet::activities::{Object, ObjectType};
    use crate::chatternet::didkey;
    use crate::db::Connector;

    use super::super::build_api;
    use super::super::test::build_message;
    use super::*;

    const NO_VEC: Option<&Vec<String>> = None;
    #[tokio::test]
    async fn api_object_updates_and_gets() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector, "did:example:server".to_string());

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        let object = Object::new(ObjectType::Note, None).await.unwrap();
        let object_id = object.id.as_ref().unwrap().as_str();
        let message = build_message(object_id, NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("GET")
            .path(&format!("/{}", object_id))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let object_back: Option<Object> = serde_json::from_slice(response.body()).unwrap();
        assert!(object_back.is_none());

        let response = request()
            .method("POST")
            .path(&format!("/{}", object_id))
            .json(&object)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("GET")
            .path(&format!("/{}", object_id))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let object_back: Option<Object> = serde_json::from_slice(response.body()).unwrap();
        let object_back = object_back.unwrap();
        assert_eq!(object_back.id, object.id);
    }

    #[tokio::test]
    async fn api_object_wont_update_invalid_object() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector, "did:example:server".to_string());

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        let object = Object::new(ObjectType::Note, None).await.unwrap();
        let object_id = object.id.as_ref().unwrap().as_str();
        let message = build_message(object_id, NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("POST")
            .path("/id:wrong")
            .json(&object)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let mut object_invalid = object.clone();
        object_invalid.members = Some(json!({"content": "abcd"}).as_object().unwrap().to_owned());
        let response = request()
            .method("POST")
            .path(&format!("/{}", object_id))
            .json(&object_invalid)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn api_object_wont_get_unknown() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector, "did:example:server".to_string());
        let response = request().method("GET").path("/id:1").reply(&api).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn api_object_wont_post_unknown() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector, "did:example:server".to_string());
        let object = Object::new(ObjectType::Note, None).await.unwrap();
        let object_id = object.id.as_ref().unwrap().as_str();
        let response = request()
            .method("POST")
            .path(&format!("/{}", object_id))
            .json(&object)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
