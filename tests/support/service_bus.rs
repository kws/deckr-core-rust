#![allow(dead_code)]
use deckr::endpoint::EndpointAddress;
use deckr::lanes::DeckrMessage;
use deckr::messaging::{Delivery, DeliveryStream, MessageTransport, ReplyInbox};
use deckr::Result;
use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_core::Stream;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
#[derive(Clone, Default)]
pub struct Bus(Arc<Mutex<BusState>>);
#[derive(Default)]
struct BusState {
    routes: BTreeMap<String, (Route, UnboundedSender<Result<Delivery>>)>,
    published: Vec<(DeckrMessage, Option<String>)>,
}
enum Route {
    Lane(String, EndpointAddress),
    Inbox,
}
struct Subscription {
    bus: Bus,
    key: String,
    receiver: UnboundedReceiver<Result<Delivery>>,
}
impl Drop for Subscription {
    fn drop(&mut self) {
        self.bus.0.lock().unwrap().routes.remove(&self.key);
    }
}
impl Stream for Subscription {
    type Item = Result<Delivery>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(cx)
    }
}
impl Bus {
    pub fn published(&self) -> Vec<DeckrMessage> {
        self.0
            .lock()
            .unwrap()
            .published
            .iter()
            .map(|(message, _)| message.clone())
            .collect()
    }
    fn register(&self, key: String, route: Route) -> DeliveryStream {
        let (sender, receiver) = unbounded();
        self.0
            .lock()
            .unwrap()
            .routes
            .insert(key.clone(), (route, sender));
        Box::pin(Subscription {
            bus: self.clone(),
            key,
            receiver,
        })
    }
    pub fn routes(&self) -> usize {
        self.0.lock().unwrap().routes.len()
    }
}
impl MessageTransport for Bus {
    async fn subscribe(&self, lane: &str, endpoint: &EndpointAddress) -> Result<DeliveryStream> {
        Ok(self.register(
            uuid::Uuid::new_v4().to_string(),
            Route::Lane(lane.into(), endpoint.clone()),
        ))
    }
    async fn open_inbox(&self) -> Result<ReplyInbox> {
        let address = format!("inbox.{}", uuid::Uuid::new_v4());
        Ok(ReplyInbox {
            messages: self.register(address.clone(), Route::Inbox),
            address,
        })
    }
    async fn publish(
        &self,
        message: &DeckrMessage,
        inbox: Option<&str>,
        reply_address: Option<&str>,
    ) -> Result<()> {
        let mut bus = self.0.lock().unwrap();
        bus.published
            .push((message.clone(), reply_address.map(str::to_owned)));
        for (key, (route, sender)) in &bus.routes {
            let matches = match (inbox, route) {
                (Some(address), Route::Inbox) => address == key,
                (None, Route::Lane(lane, endpoint)) => {
                    *lane == message.lane && message.recipient.targets_endpoint(endpoint)?
                }
                _ => false,
            };
            if matches {
                let _ = sender.unbounded_send(Ok(Delivery {
                    message: message.clone(),
                    reply_address: reply_address.map(str::to_owned),
                }));
            }
        }
        Ok(())
    }
}
