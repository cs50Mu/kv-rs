use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            // TODO: 这个 default 是在哪里定义的？
            // 在abi.rs中没有看到定义，难道是 prost 库实现的？
            // 解答：是 prost 库实现的，具体是#[derive(::prost::Message)]做的
            // 代码现在还看不懂，通过"喜欢程序的历史君"确认了 prost 生成的类型都是
            // derive 了 Default trait 的。
            // https://www.bilibili.com/video/BV1FL4y1x7MU?spm_id_from=333.999.0.0&vd_source=c3e98a24f8faea4be1ff057f8fa301fe
            // 根据定义，Value 类型的 value 字段的 default 值是 None
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(v) => Value::from(v).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.get(&self.table, key) {
                Ok(Some(v)) => Ok(v),
                Ok(None) => Ok(Value::default()),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<Value>, KvError>>()
            .into()
    }
}

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        // 注意这个技巧，在下面直接写self.x的话，borrow checker 会报错
        // store.set函数的参数：table需要的是引用，key 和 value 需要的是 move 过去
        // 而它们又在同一个结构体内，所以用 self 是不行的
        // 只能把它们分拆开

        let table = self.table;
        let pairs = self.pairs;
        pairs
            .into_iter()
            .map(
                |kv| match store.set(&table, kv.key, kv.value.unwrap_or_default()) {
                    Ok(Some(v)) => Ok(v),
                    Ok(None) => Ok(Value::default()),
                    Err(e) => Err(e),
                },
            )
            .collect::<Result<Vec<Value>, KvError>>()
            .into()
    }
}

impl CommandService for Hmexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.contains(&self.table, key) {
                // TODO: 有没有更优雅的写法？
                Ok(v) => Ok(v.into()),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<Value>, KvError>>()
            .into()
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.del(&self.table, key) {
                Ok(Some(v)) => Ok(v),
                Ok(None) => Ok(Value::default()),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<Value>, KvError>>()
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 下面这行的作用是，标识下面的函数是一个单元测试
    // 所以测试里的辅助函数不应该标注这个
    // https://doc.rust-lang.org/book/ch11-01-writing-tests.html
    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        // set
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);
        // get
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
        // del
        let cmd = CommandRequest::new_hdel("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
        // get again
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        // before set, it's not exist yet
        let cmd = CommandRequest::new_hexist("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[false.into()], &[]);
        // set
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);
        // after set, it should be exist
        let cmd = CommandRequest::new_hexist("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[true.into()], &[]);
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("u1", 10), ("u2", 20)], &store);
        let cmd = CommandRequest::new_hmexist("t1", vec!["u2".into(), "u3".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[true.into(), false.into()], &[]);
    }

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    #[test]
    fn hmset_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("u1", "world")], &store);
        let cmd = CommandRequest::new_hmset("t1", vec![("u1", 10), ("u2", 20)]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into(), Value::default()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
    }

    #[test]
    fn hmget_should_work() {
        let store = MemTable::new();
        set_key_pairs("score", vec![("u1", 10), ("u2", 20)], &store);
        let cmd = CommandRequest::new_hmget("score", vec!["u2".into(), "u3".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[20.into(), Value::default()], &[]);
    }

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();
        set_key_pairs("score", vec![("u1", 10), ("u2", 20), ("u3", 30)], &store);
        let cmd = CommandRequest::new_hmdel("score", vec!["u2".into(), "u3".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[20.into(), 30.into()], &[]);
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
            CommandRequest::new_hset("score", "u1", 6.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hgetall("score");
        let res = dispatch(cmd, &store);
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs);
    }

    fn set_key_pairs<T: Into<Value>>(table: &str, pairs: Vec<(&str, T)>, store: &impl Storage) {
        pairs
            .into_iter()
            .map(|(k, v)| CommandRequest::new_hset(table, k, v.into()))
            .for_each(|cmd| {
                dispatch(cmd, store);
            });
    }
}
