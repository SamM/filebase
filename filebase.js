var fs = require('fs'),
	events = require('events'),
	async = require('async');

function to_array(args) {
	var len = args.length,
		arr = new Array(len), i;

	for (i = 0; i < len; i += 1) {
		arr[i] = args[i];
	}

	return arr;
}
function FileBase(base_dir, on_init){
	if(!(this instanceof FileBase)) return new FileBase(base_dir, on_init);
	if(!base_dir) throw new Error('You must supply a directory path');
	
	var DB = this;
	var BASE_DIR_EXISTS = null;

	function cb(callback){
		callback = typeof callback == 'function'?callback:function(){};
		return callback.apply(DB, to_array(arguments).slice(1));
	}
	
	this.base_dir = base_dir || '';
	this.db_keys = [];
	this.ee = new events.EventEmitter();
	this.emit = function(){ this.ee.emit.apply(this, to_array(arguments)); return this; };
	this.on = function(){ this.ee.on.apply(this, to_array(arguments)); return this; };
	
	this.file_path = function(key){ return [this.base_dir, key].join("/"); };
	this.handleError = function(action, err, a1, a2, a3){
		if(this.DEBUG) console.log(['<< DATABASE ERROR',action, !!err?1:0, !!a1?1:0, !!a2?1:0, !!a3?1:0].join(' >> '))
		return err;
	};
	this.DEBUG = 1;
	
	this.validations = {
		'base_dir': function(dir){
			dir = dir || this.base_dir || '';
			if(dir.indexOf('/')==0) dir = '.'+dir;
			else if(dir.indexOf('./')!=0) dir = './'+dir;
			return dir;
		}
	};
	this.validate = function(id){
		var validation = this.validations[id];
		if(typeof validation != 'function') return undefined;
		return validation.apply(this, [].slice.call(arguments,1));
	}
	this.check = function(callback){
		if(this.base_exists()==true) cb(callback, null);
		else this.init(null, function(err){ cb(callback, err||'Database is empty'); });
		return this;
	}
	this.base_exists = function(set){
		if(typeof set == 'boolean') BASE_DIR_EXISTS = set;
		return BASE_DIR_EXISTS;
	};

	this.init = function(base_dir, callback){
		base_dir = this.validate('base_dir', base_dir||this.base_dir);
		this.base_dir = base_dir;
		this.db_keys = [];
		fs.stat(base_dir, function(err, dir){
			if(err || (dir && !dir.isDirectory())) fs.mkdir(base_dir, 0777, function(err){
				if(err) err = DB.handleError('init', err, base_dir);
				else DB.base_exists(true);
				cb(callback, err, base_dir);
			});
			else if(dir && dir.isDirectory()){
				DB.base_exists(true);
				DB.load_keys(function(e, keys){
					cb(callback, e, keys, base_dir);
				});
			};
		});
		return this;
	};

	this.jget = function(key, callback){
		callback = callback || function(){};
		this.check(function(err){
			if(err) cb(callback, err);
			else this.get(key, function(err, contents){
				contents = JSON.parse(contents);
				cb(callback, err, contents);
				return contents;
			});
		});
		return this;
	};
	this.get = function(key, callback){
		var path = this.file_path(key);
		this.check(function(err){
			if(err) cb(callback, err);
			else fs.readFile(path, function(err, contents){
			   if(err) err = DB.handleError('get', err, contents);
			   cb(callback, err, contents.toString(), key);
			});
		});
		return this;
	};
	this.jset = function(key, object, callback){
		this.check(function(){
			object = JSON.stringify(object);
			this.set(key, object, callback);
		});
		return this;
	};
	this.set = function(key, value, callback){
		var path = this.file_path(key);
		this.check(function(){
			fs.writeFile(path, value, function(err){
				if(err) err = DB.handleError('set', err);
				cb(callback, err, value, key);
			});
		});
		return this;
	};
	this.stat = function(key, callback){
		var path = this.file_path(key);
		fs.stat(path, function(err, stats){
			cb(callback, err, stats);
		});
		return this;
	};
	this.hmake = function(key, callback){
		this.isHash(key, function(err, is_hash, stats){
			if(is_hash) cb(callback, null);
			else {
				if(stats && stats.isFile()) cb(callback, this.handleError('hmake', err || 'Key '+key+' is not a hash'));
				else fs.mkdir(this.file_path(key), 0777, function(mkdir_err){
					if(mkdir_err) mkdir_err = DB.handleError('hmake', mkdir_err);
					cb(callback, mkdir_err);
				});
			};
		});
		return this;
	};
	this.isHash = function(key, callback){
		this.check(function(base_empty){
			if(base_empty) cb(callback, base_empty);
			else this.stat(key, function(err, stat){
				cb(callback, err, stat&&stat.isDirectory(), stat);
			});
		});
		return this;
	};
	this.hset = function(key, hkey, value, callback){
		this.hmake(key, function(err){
			if(err) cb(callback, DB.handleError('hmake', err));
			else this.set([key,hkey].join('/'), value, callback);
		});
		return this;
	};
	this.happend = function(key, hkey, value, callback){
		this.hmake(key, function(err){
			if(err) cb(callback, DB.handleError('happend', err));
			else this.append([key,hkey].join('/'), value, callback);
		});
		return this;
	};
	this.hget = function(key, hkey, callback){
		this.isHash(key, function(err, is_hash){
			if(err || !is_hash) cb(callback, DB.handleError('hget', err||'Key '+key+' is not a hash'));
			else this.get([key,hkey].join('/'), callback);
		});
		return this;
	};
	this.hjget = function(key, hkey, callback){
		this.isHash(key, function(err, is_hash){
			if(err || !is_hash) cb(callback, DB.handleError('hjget', err||'Key '+key+' is not a hash'));
			else this.jget([key,hkey].join('/'), callback);
		});
		return this;
	};
	this.hjset = function(key, hkey, object, callback){
		this.hmake(key, function(err){
			if(err) cb(callback, DB.handleError('hjset', err));
			else this.jset([key,hkey].join('/'), object, callback);
		});
		return this;
	};
	this.hgetall = function(key, callback){
		this.hkeys(key, function(err, keys){
			if(err) cb(callback, this.handleError('hgetall', err, keys));
			else {
				var series = {};
				keys.forEach(function(hkey){ series[hkey] = function(calbk){ DB.get([key, hkey].join('/'), calbk); }; });
				async.parallel(series, function(get_errs, results){ cb(callback, get_errs, results); });
			};
		});
		return this;
	};
	this.hjgetall = function(key, callback){
		this.hkeys(key, function(err, keys){
			if(err) cb(callback, this.handleError('hjgetall', err, keys));
			else {
				var series = {};
				keys.forEach(function(hkey){ series[hkey] = function(calbk){ DB.jget([key, hkey].join('/'), calbk); }; });
				async.parallel(series, function(jget_errs, results){ cb(callback, jget_errs, results); });
			};
		});
		return this;
	};
	this.hlen = function(key, callback){
		this.hkeys(key, function(err, keys){
			if(err) cb(callback, this.handleError('hlen', err), keys);
			else cb(callback, null, keys.length, keys);
		});
		return this;
	};
	this.hkeys = function(key, callback){
		var path = this.file_path(key);
		this.isHash(key, function(err, is_hash){
			if(err || !is_hash) cb(callback, DB.handleError('hkeys', err||'Key '+key+' is not a hash'));
			else fs.readdir(path, function(readdir_err, keys){
				if(readdir_err) readdir_err = DB.handleError('hkeys', readdir_err, keys);
				cb(callback, readdir_err, keys);
			});
		});
		return this;
	};
	this.hdel = function(key, hkey, callback){
		var path = this.file_path([key,hkey].join('/'));
		this.check(function(base_empty){
			if(base_empty) cb(callback, base_empty);
			else fs.unlink(path, function(err){
				if(err) err = DB.handleError('hdel', err);
				cb(callback, err);
			});
		});
		return this;
	};
	this.hflush = function(key, callback){
		this.hkeys(key, function(err, keys){
			if(err) cb(callback, err);
			else{
				var series = [];
				function hdel(hkey){ return function(calbk){ DB.hdel(key, hkey, calbk); }; };
				for(var i = 0; i<keys.length; i++) series.push(hdel(keys[i]));
				async.parallel(series, function(err, result){
					if(err) err = DB.handleError('hflush', err);
					cb(callback, err);
				});
			}
		});
		return this;
	};
	this.hdestroy = function(key, callback){
		var path = this.file_path(key);
		this.hflush(key, function(flush_err){
			if(flush_err) cb(callback, DB.handleError('hdestroy', flush_err));
			else fs.rmdir(path, function(err){
				if(err) err = DB.handleError('hdestroy', err);
				cb(callback, err);
			});
		});
		return this;
	};
	this.del = function(key, callback){
		var path = this.file_path(key);
		this.check(function(err){
			if(err) cb(callback, err);
			else fs.stat(path, function(err, stats){
				if(err) cb(callback, err);
				else{
					if(stats.isDirectory()) DB.hdestroy(key, function(err){
						if(err) err = DB.handleError('del', err);
						cb(callback, err);
					});
					else fs.unlink(path, function(err){
						if(err) err = DB.handleError('del', err);
						cb(callback, err);
					});
				}
			})
		});
		return this;
	};
	this.exists = function(key, callback){
		if(!callback) return this;
		this.load_keys(function(err, keys){
			callback(keys.indexOf(key)>-1);
		});
		return this;
	};
	this.rename = function(key, new_key, callback){
		this.check(function(err){
			if(err) cb(callback, err);
			else this.exists(key, function(exists){
				if(exists) this.get(key, function(err, contents){
					if(!err) this.set(new_key, contents, function(err){
						this.del(key, function(err){ cb(callback, err); });
					});
					else cb(callback, DB, err);
				});
				else cb(callback, DB.handleError('rename', 'Key `'+key+'` does not exist'))
			});
		});
		return this;
	};
	this.renamenx = function(key, new_key, callback){
		this.check(function(err){
			if(err) cb(callback, err);
			else this.exists(new_key, function(exists){
				if(exists) cb(callback, this.handleError('renamenx', 'Key `'+new_key+'` already exists'));
				else this.rename(key, new_key, callback);
			});
		});
		return this;
	};
	this.append = function(key, value, callback){
		var path = this.file_path(key);
		var buff,len;
		if(value instanceof Buffer) buff = value;
		else{
			buff = new Buffer(value);
			len = buff.write(value, 0, 'utf8');
		}
		len = len || buff.length;
		
		fs.open(path, 'a', 0666, function(err, fd){
			if(err) cb(callback, DB.handleError('append', err, fd), fd);
			else fs.write(fd, buff, 0, len, null, function(write_err, written, buffer){
				if(write_err) write_err = DB.handleError('append', write_err, written, buffer);
				fs.close(fd, function(err){
					if(err) err = DB.handleError('append', err, write_err);
					else err = write_err || null;
					cb(callback, err, buffer?buffer.toString():buffer, written);
				});
			});
		});
		return this;
	};
	this.flush = function(callback){
		this.check(function(err){
			if(err) cb(callback, err);
			else this.load_keys(function(err, keys){
				var done = 0, kl = keys.length, errors = [],
					on_done = function(){
						if(errors.length>0) errors = DB.handleError('flush', errors);
						else errors = null;
						if(callback) cb(callback, errors);
					},
					on_delete = function(err){done++; if(err) errors.push(err); if(done==kl) on_done(); };
				if(!kl) return on_done();
				for(var i=0; i<kl; i++) this.del(keys[i], on_delete);
			});
		});
		return this;
	};
	this.destroy = function(callback){
		if(!this.base_exists()){ 
			cb(callback);
			return this;
		}
		var path = this.base_dir;
		this.flush(function(e){
			if(!e) fs.rmdir(path, function(err){
				if(err) err = DB.handleError('destroy', err);
				else DB.base_exists(false);
				cb(callback, err);
			});
		});
		return this;
	};
	this.remove = this.del;
	this.require = function(key, callback){
		this.check(function(err){
			if(err) cb(callback, err);
			else cb(callback, require(this.file_path(key)));
		});
		return this;
	};
	this.load_keys = function(callback){
		this.check(function(err){
			if(err) cb(callback, null, []);
			else fs.readdir(this.base_dir, function(err, contents){
				if(err) err = DB.handleError('load_keys', err, contents);
				else DB.db_keys = contents;
				cb(callback, err, contents);
			});
		});
		return this;
	};
	
	this.init(base_dir, on_init);
}
FileBase.create = function(base_dir, on_init){
	return new FileBase(base_dir, on_init);
};
module.exports = FileBase;