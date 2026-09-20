//! Own the non-Send SDK on a dedicated compio thread. Only owned request
//! parameters and results cross threads; no Send wrapper or unsafe impl.
use std::rc::Rc;

use autumn_client::{AutumnError, ClusterClient};
use futures::{future::LocalBoxFuture, FutureExt, StreamExt};

type Job = Box<dyn FnOnce(Rc<ClusterClient>) -> LocalBoxFuture<'static, ()> + Send>;

#[derive(Clone)]
pub(crate) struct Bridge(flume::Sender<Job>);

impl Bridge {
    pub async fn connect(
        manager: String,
        scope: String,
        credential: Option<(String, Vec<u8>)>,
    ) -> object_store::Result<Self> {
        let (tx, rx) = flume::bounded::<Job>(32);
        let (ready_tx, ready_rx) = futures::channel::oneshot::channel();
        std::thread::Builder::new()
            .name("autumn-object-store".into())
            .spawn(move || {
                let runtime = match compio::runtime::Runtime::new() {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        let _ = ready_tx.send(Err(super::error(e)));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let client = match credential {
                        Some((principal, secret)) => {
                            ClusterClient::connect_with_credential(
                                &manager, &scope, &principal, secret,
                            )
                            .await
                        }
                        None => ClusterClient::connect(&manager, &scope).await,
                    };
                    let client = match client {
                        Ok(client) => Rc::new(client),
                        Err(e) => {
                            let _ = ready_tx.send(Err(super::error(e)));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(())).is_err() {
                        return;
                    }
                    rx.into_stream()
                        .map(|job| job(client.clone()))
                        .buffer_unordered(32)
                        .for_each(|()| async {})
                        .await;
                });
            })
            .map_err(super::error)?;
        ready_rx.await.map_err(super::error)??;
        Ok(Self(tx))
    }

    pub async fn call<T, F>(&self, job: F) -> object_store::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Rc<ClusterClient>) -> LocalBoxFuture<'static, Result<T, AutumnError>>
            + Send
            + 'static,
    {
        let (tx, rx) = futures::channel::oneshot::channel();
        self.0
            .send_async(Box::new(move |client| {
                async move {
                    let _ = tx.send(job(client).await);
                }
                .boxed_local()
            }))
            .await
            .map_err(|_| super::error("Autumn worker stopped"))?;
        rx.await.map_err(super::error)?.map_err(super::error)
    }
}
