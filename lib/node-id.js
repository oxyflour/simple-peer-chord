const BigInt = require('big-integer'),
	crypto = require('crypto')

function NodeId(BITS) {
	if (!(this instanceof NodeId)) {
		return new NodeId(BITS)
	}

	this.BITS = BITS || 160

	this.MAX_ID = BigInt(1).shiftLeft(this.BITS)

	this.POW_OF_TWO = Array(this.BITS).fill(0)
		.map((_, i) => BigInt(1).shiftLeft(i))
}

NodeId.prototype.fix = function(id) {
	var len = this.BITS / 8 * 2
	while (id.length < len) id = '0' + id
	return id.length > len ? id.slice(-len) : id
}

NodeId.prototype.create = function(id) {
	return this.fix((id ? BigInt(id, 16) : BigInt.randBetween(0, this.MAX_ID)).toString(16))
}

NodeId.prototype.hash = function(val) {
	return this.fix(crypto.createHash('sha1').update(JSON.stringify(val)).digest().toString('hex'))
}

NodeId.prototype.fingerStart = function(id, offset) {
	return this.fix(BigInt(id, 16).add(this.POW_OF_TWO[offset]).toString(16))
}

NodeId.prototype.inRange = function(begin, id, end) {
	if (begin && id && end) {
		if (begin === end) {
			return true
		}
		else if (begin > end) {
			return id > begin || id < end
		}
		else {
			return id > begin && id < end
		}
	}
}

module.exports = new NodeId()