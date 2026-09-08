/// A payload and the source cursor that can resume after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<T, C> {
    pub cursor: C,
    pub payload: T,
}

impl<T, C> Record<T, C> {
    pub fn new(cursor: C, payload: T) -> Self {
        Self { cursor, payload }
    }
}
