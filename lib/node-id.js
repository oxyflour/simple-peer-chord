const BigInt = require('big-integer'),
	crypto = require('crypto')

function NodeId(BITS, DISP) {
	if (!(this instanceof NodeId))
		return new NodeId(BITS, DISP)

	this.BITS = BITS || 160

	this.DISP = DISP || 16

	this.MAX_ID = BigInt(1).shiftLeft(this.BITS)

	this.POW_OF_TWO = Array(this.BITS).fill(0)
		.map((_, i) => BigInt(1).shiftLeft(i))
}

NodeId.prototype.create = function(id) {
	id = id ?
		BigInt(id, this.DISP).mod(this.MAX_ID) :
		BigInt.randBetween(0, this.MAX_ID)
	return id.toString(this.DISP)
}

NodeId.prototype.hash = function(val) {
	var hex = crypto.createHash('sha1')
		.update(JSON.stringify(val)).digest().toString('hex')
	return this.create(hex)
}

NodeId.prototype.fingerStart = function(id, offset) {
	id = BigInt(id, this.DISP).add(this.POW_OF_TWO[offset]).mod(this.MAX_ID)
	return id.toString(this.DISP)
}

NodeId.prototype.inRange = function(begin, id, end) {
	id  = BigInt(id, this.DISP)
	begin = BigInt(begin, this.DISP)
	end = BigInt(end, this.DISP)
	if (begin.gt(0) && id.gt(0) && end.gt(0)) {
		if (begin.eq(end))
			return true
		else if (begin.gt(end))
			return id.gt(begin) || id.lt(end)
		else
			return id.gt(begin) && id.lt(end)
	}
}

module.exports = NodeId