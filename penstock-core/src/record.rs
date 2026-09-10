/// A payload and its immutable source-local position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<T, P> {
    pub payload: T,
    position: P,
}

impl<T, P> Record<T, P> {
    pub fn new(position: P, payload: T) -> Self {
        Self { position, payload }
    }

    /// Inspect the source position without changing it.
    pub fn position(&self) -> &P {
        &self.position
    }

    /// Consume the envelope into its position and payload.
    pub fn into_parts(self) -> (P, T) {
        (self.position, self.payload)
    }
}
