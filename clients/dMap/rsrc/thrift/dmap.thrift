
namespace java ch.usi.da.dmap.thrift.gen

exception MapError {
	1: string errorMsg,
}

enum CommandType {
    GET = 0,
    PUT = 1,
    REMOVE = 2,
    SIZE = 3,
    CLEAR = 4,
    CONTAINSVALUE = 5,
    FIRSTKEY = 6,
    LASTKEY = 7;
}

struct Command {
  1: i64 id,
  2: CommandType type,
  3: optional binary key,
  4: optional binary value,
}

struct Response {
  1: i64 id,
  2: i64 count,
  3: optional binary key,
  4: optional binary value,
}

enum RangeType {
    CREATERANGE = 1,
    PERSISTRANGE = 2,
    GETRANGE = 3,
    DELETERANGE = 4
}

struct RangeCommand {
  1: i64 id,
  2: RangeType type,
  3: optional binary fromkey,
  4: optional binary tokey,
  5: optional i32 fromid,
  6: optional i32 toid,
  7: optional i64 snapshot 
}

struct RangeResponse {
  1: i64 id,
  2: i64 count,
  3: i64 snapshot,
  4: optional binary values
}


service Dmap {
	Response execute(1: Command cmd) throws (1: MapError e),
	RangeResponse range(1: RangeCommand cmd) throws (1: MapError e),
	//partition
		
}


//	// single-partition
//	V get(Object key)
//	V put(K key, V value)
//	V remove(Object key)
//	
//	// multi-partition commands
//	int size()
//	boolean containsValue(Object value)
//	void clear()
//
//	// global snapshot/iterator commands
//	K firstKey()
//	K lastKey()
//	SortedMap<K, V> subMap(K fromKey, K toKey)
//	SortedMap<K, V> headMap(K toKey)
//	SortedMap<K, V> tailMap(K fromKey)
//	Set<K> keySet()
//	Collection<V> values()
//	Set<java.util.Map.Entry<K, V>> entrySet()
