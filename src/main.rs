use sled::{Config, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
//use std::result::Result;


struct PersistenceManager {
    root_path: String,
    Databases: HashMap<String, sled::Db>,
}

impl PersistenceManager {

    pub fn push_item(&mut self, queue_name: String, body: String) -> Result<Box<Vec<u8>>> {
        let db = self.Databases.get(&queue_name).unwrap();
        let key = format!("{}:{}", queue_name, db.generate_id().unwrap());
        let res = db.insert(key, body.as_bytes());
        let res = res.unwrap();
        let b = res.unwrap();//.as_ref();
        Ok(Box::new(b.to_vec()))
    }

    pub fn pop_item(&mut self, queue_name: String) -> Result<Box<Vec<u8>>> {
        let db = self.Databases.get(&queue_name).unwrap();
        let (_,  value) = db.first().unwrap().unwrap();
        Ok(Box::new(value.as_ref().to_vec()))
    }

    pub fn load_or_create_database(&mut self, queue_name: String, dir: PathBuf) -> Result<()> {
        let config = Config::new().path(dir);
        let db = config.open()?;
        self.Databases.insert(queue_name, db);
        Ok(())
    }

    pub fn setup(&mut self) {
        if !Path::new(&self.root_path).exists() {
            fs::create_dir_all(&self.root_path).unwrap();
        }
        
        self.load_persistence(); 
    }

    fn load_persistence(&mut self) {
        let dir = Path::new(&self.root_path);
            if dir.is_dir() {
                for entry in fs::read_dir(dir).unwrap() {
                    //let entry = entry?;
                    let path = entry.unwrap().path();
                if path.is_dir() {
                    let queue_name = path.to_str().unwrap().to_string();
                    self.load_or_create_database(queue_name, path);    
                };
            }
        }
    }

    pub fn new(basepath: String) -> Self {
        let mut s = Self {
            root_path: basepath,
            Databases: HashMap::new(),
        };
        s.setup();
        return s
    }
    
}


fn main() {
    let mut pm = PersistenceManager::new("lero".to_string());
    let _ = pm.push_item("test".to_string(), "nheco".to_string());
}
