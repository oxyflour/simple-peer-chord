// TODO: support bittorrent-id

function NodeId() {
}

var BITS =
NodeId.BITS = 30

var MAX_ID =
NodeId.MAX_ID = 1 << BITS

var DISP =
NodeId.DISP = 16

NodeId.from = function(val) {
	val = parseInt(val, DISP) || Math.floor(Math.random() * MAX_ID)
	if (!(val > 0 && val < MAX_ID)) throw 'invalid id'
	return val.toString(DISP)
}

NodeId.shift = function(id, offset) {
	val = (parseInt(id, DISP) + (1 << offset)) % MAX_ID
	if (!(val > 0 && val < MAX_ID)) throw 'invalid id'
	return val.toString(DISP)
}

NodeId.inRange = function(id1, id, id2) {
	id  = parseInt(id,  DISP) % MAX_ID
	id1 = parseInt(id1, DISP) % MAX_ID
	id2 = parseInt(id2, DISP) % MAX_ID
	if (id1 >= 0 && id >= 0 && id2 >= 0) {
		if (id1 === id2)
			return true
		else if (id1 > id2)
			return id > id1 || id < id2
		else
			return id1 < id && id < id2
	}
}

module.exports = NodeId