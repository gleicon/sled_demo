use sled::{Config, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
//use std::result::Result;


struct PersistenceManager <'a> {
    root_path: String,
    path:  &'a Path,
    databases: HashMap<String, sled::Db>,
}

impl<'a> PersistenceManager<'a> {

    pub fn push_item(&mut self, queue_name: String, body: String) -> Result<Box<Vec<u8>>> {
        let mut db = self.databases.get(&queue_name.clone());
        match db.clone() {
            Some(_) => (),
            None => {
                self.load_or_create_database(queue_name.clone()).unwrap();
                db = self.databases.get(&queue_name);
            },
        }
        let db = db.unwrap();

        let key = format!("{}:{}", queue_name, db.generate_id().unwrap());
        let res = db.insert(key, body.as_bytes());
        let res = res.unwrap();
        match res {
            Some(b) => Ok(Box::new(b.to_vec())),
            None => Ok(Box::new([].to_vec())),
        }
    }

    pub fn pop_item(&mut self, queue_name: String) -> Result<Box<Vec<u8>>> {
        let db = self.databases.get(&queue_name).unwrap();
        let (_,  value) = db.first().unwrap().unwrap();
        Ok(Box::new(value.as_ref().to_vec()))
    }

    pub fn load_or_create_database(&mut self, queue_name: String) -> Result<()> {
        let mut pb = PathBuf::new();
        pb.push(self.path);
        pb.push(queue_name.clone());
        let config = Config::new().path(pb);
        let db = config.open()?;
        self.databases.insert(queue_name, db);
        Ok(())
    }

    pub fn setup(&mut self) {
        if !Path::new(&self.root_path).exists() {
            fs::create_dir_all(&self.root_path).unwrap();
        }
        
        self.load_persistence(); 
    }

    fn load_persistence(&mut self) {
        let dir = self.path;
            if dir.is_dir() {
                for entry in fs::read_dir(dir).unwrap() {
                    //let entry = entry?;
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        let queue_name = path.to_str().unwrap().to_string();
                        self.load_or_create_database(queue_name).unwrap();    
                    };
            }
        }
    }

    pub fn new (basepath:  &'a String) -> Self {
        let bp = Path::new(basepath);
        let mut s = Self {
            root_path: basepath.clone(),
            databases: HashMap::new(),
            path: bp,
        };
        s.setup();
        return s
    }
}


fn main() {
    let path = "lero".to_string();
    let mut pm = PersistenceManager::new(&path);
    let _ = pm.push_item("test".to_string(), "nheco".to_string());
    print!("{}", format!("{:?}", pm.pop_item("test".to_string())));
}
