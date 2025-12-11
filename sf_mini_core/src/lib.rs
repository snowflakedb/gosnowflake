use std::os::raw::c_char;

/// Returns the full version string for sf_core.
///
/// This function returns a pointer to a static null-terminated string
/// containing the version of sf_core.
///
/// @return A pointer to a static string containing the version.
///         The caller must NOT free this pointer.
///         The returned string is valid for the lifetime of the program.
///
/// @note Thread-safe: Yes
/// @note This function never returns NULL
///
/// Example usage:
/// @code
///   const char* version = sf_core_full_version();
///   printf("Version: %s\n", version);
/// @endcode
///
/// # Safety
///
/// The returned pointer points to a static string that is valid for the lifetime
/// of the program. The caller must not free the returned pointer.
#[unsafe(no_mangle)]
pub extern "C" fn sf_core_full_version() -> *const c_char {
    // Static version string - update this as needed
    static VERSION: &str = "0.0.1\0";
    VERSION.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn test_sf_core_full_version() {
        let version_ptr = sf_core_full_version();
        assert!(!version_ptr.is_null());

        unsafe {
            let version_cstr = CStr::from_ptr(version_ptr);
            let version_str = version_cstr.to_str().unwrap();
            assert!(!version_str.is_empty());
            assert_eq!(version_str, "0.0.1");
        }
    }
}
