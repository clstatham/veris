/// A macro to create a wrapper struct around another type.
/// It provides a constructor, an accessor for the inner value,
/// and deref implementations for easy access to the inner type.
///
/// # Example
///
/// ```rust
/// #[macro_use]
/// extern crate veris_db;
/// wrap! {
///     #[derive(Debug, Clone)]
///     pub struct MyWrapper(String);
/// }
/// ```
#[macro_export]
macro_rules! wrap {
    {
        $(
        $(#[derive($($derive:ident),*)])?
        $outer_vis:vis struct $wrapper:ident($inner_vis:vis $inner_type:ty);
        )*
    } => {
        $(
            $(#[derive($($derive),*)])?
            #[repr(transparent)]
            $outer_vis struct $wrapper($inner_type);

            impl $wrapper {
                /// Creates a new instance of the wrapper.
                #[inline]
                pub fn new(inner: $inner_type) -> Self {
                    Self(inner)
                }

                /// Returns the inner value.
                #[inline]
                pub fn inner(&self) -> &$inner_type {
                    &self.0
                }

                /// Consumes the wrapper and returns the inner value.
                #[inline]
                pub fn into_inner(self) -> $inner_type {
                    self.0
                }
            }

            impl std::ops::Deref for $wrapper {
                type Target = $inner_type;

                #[inline]
                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }

            impl std::ops::DerefMut for $wrapper {
                #[inline]
                fn deref_mut(&mut self) -> &mut Self::Target {
                    &mut self.0
                }
            }
        )*
    };
}
