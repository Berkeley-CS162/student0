/// File-backed persister using atomic rename.
pub struct Persister {
    path: std::path::PathBuf,
}

impl Persister {
    pub fn new(path: std::path::PathBuf) -> Self {
        Persister { path }
    }

    /// Replace any previous content.
    pub fn save(&self, data: &str) {
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, data).unwrap();
        std::fs::rename(&tmp, &self.path).unwrap();
    }

    /// Return `""` if no state has been saved.
    pub fn read(&self) -> String {
        std::fs::read_to_string(&self.path).unwrap_or_default()
    }
}
