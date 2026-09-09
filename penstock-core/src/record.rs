/// A payload and its source-local position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<T, P> {
    pub position: P,
    pub payload: T,
}

impl<T, P> Record<T, P> {
    pub fn new(position: P, payload: T) -> Self {
        Self { position, payload }
    }
}
