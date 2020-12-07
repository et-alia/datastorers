use gcp_auth::{Error as GCPAuthError, Token};
use google_api_auth::GetAccessToken;

use std::error::Error;

#[derive(Debug)]
pub(crate) struct GoogleToken(pub Token);

impl GetAccessToken for GoogleToken {
    fn access_token(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        Ok(self.0.as_str().to_string())
    }
}

impl GoogleToken {
    #[tokio::main]
    pub(crate) async fn get_token() -> Result<Self, GCPAuthError> {
        let authentication_manager = gcp_auth::init().await?;
        let token = authentication_manager
            .get_token(&[
                "https://www.googleapis.com/auth/datastore",
            ])
            .await?;
        Ok(Self(token))
    }
}