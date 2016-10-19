var tape = require('tape'),
	co = require('co'),
	wrtc = require('electron-webrtc')({ headless: true }),
	Chord = require('../lib/chord'),
	NodeId = require('../lib/node-id')()

var chords = [ ],
	interval = 3000,
	opts = {
		peerOptions: {
			wrtc,
		},
	}

wrtc.on('error', err => {
	console.error(err)
})

Array.prototype.hasSameElements = function() {
	return this.every(item => item === this[0])
}

function sleep(time) {
	return new Promise(resolve => setTimeout(resolve, time))
}

function test(message, num, gen) {
	tape(message, t => {
		t.plan(typeof num === 'function' ? num() : num)
		co(gen(t)).catch(err => console.error(err))
	})
}

var peerCount = 6
test('create nodes', 4 * peerCount, function *(t) {
	var remaining = peerCount
	while (remaining -- > 0) {
		var chord = new Chord(opts, chords[0] || true)

		console.log('node added, ' + remaining + ' remainning')
		chords.push(chord)

		yield new Promise(resolve => chord.on('ready', resolve))
		yield sleep(interval)

		var idToQuery = NodeId.create()
		t.ok((yield chords.map(c => c.node.findPredecessorId(idToQuery))).hasSameElements())
		t.ok((yield chords.map(c => c.node.findSuccessorId(idToQuery))).hasSameElements())
		t.deepEqual((yield chords.map(c => c.node.findPredecessorId(c.id))), chords.map(c => c.node.predecessorId))
		t.deepEqual((yield chords.map(c => c.node.findSuccessorId(c.id))), chords.map(c => c.node.id))
	}
})

test('routing', _ => chords.length * (chords.length - 1), function *(t) {
	chords.forEach(c => c.on('test-routing', data => {
		t.equal(data, c.id)
	}))

	chords.forEach(c => chords.forEach(s => {
		if (c !== s) s.send(c.id, 'test-routing', c.id)
	}))
})

test('multi-routing', _ => chords.length, function *(t) {
	var rand = Math.random()

	chords.forEach(c => c.on('test-multi-routing', data => t.equal(data, rand)))
	chords[0].send(chords.map(c => c.id), 'test-multi-routing', rand)
})

test('put/get #1', 1, function *(t) {
	chords[0].put('hello', 'world')
	yield sleep(interval)

	t.deepEqual((yield chords.map(c => c.get('hello'))), chords.map(c => 'world'))
})

test('put/get #2', 1, function *(t) {
	chords[0].put('hello', 'world!')
	yield sleep(interval)

	t.deepEqual((yield chords.map(c => c.get('hello'))), chords.map(c => 'world!'))
})

var channel = '1'

test('subscribe/publish #1', _ => 3 * (chords.length - 1), function *(t) {
	yield chords.slice(0, 3).map(c => c.subscribe(channel))
	yield sleep(interval)

	chords.forEach(c => c.on('test-sub-pub', ch => t.equal(ch, channel)))
	chords.forEach(c => c.publish(channel, 'test-sub-pub', channel))
})

test('subscribe/publish #2', _ => 2 * (chords.length - 1), function *(t) {
	yield chords[0].unsubscribe(channel)
	yield sleep(interval)

	chords.forEach(c => c.on('test-sub-pub2', ch => t.equal(ch, channel)))
	chords.forEach(c => c.publish(channel, 'test-sub-pub2', channel))
})

test('node failure', 3, function *(t) {
	var keys = chords.map(c => Object.keys(c.node.storage)).filter(keys => keys[0]),
		chord = chords.filter(c => c.node.isResponsibleFor(keys[0]))[0]
	chord.stop()
	chords.splice(chords.indexOf(chord), 1)

	yield sleep(chord.hub.opts.peerCallTimeout * 1.5)

	var idToQuery = NodeId.create()
	t.ok((yield chords.map(c => c.node.findPredecessorId(idToQuery))).hasSameElements())
	t.ok((yield chords.map(c => c.node.findSuccessorId(idToQuery))).hasSameElements())
	t.deepEqual((yield chords.map(c => c.get('hello'))), chords.map(c => 'world!'))
})

test('node rejoin', 3, function *(t) {
	var chord = new Chord(opts, chords[0])
	chords.push(chord)

	yield new Promise(resolve => chord.on('ready', resolve))
	yield sleep(interval)

	var idToQuery = NodeId.create()
	t.ok((yield chords.map(c => c.node.findPredecessorId(idToQuery))).hasSameElements())
	t.ok((yield chords.map(c => c.node.findSuccessorId(idToQuery))).hasSameElements())
	t.deepEqual((yield chords.map(c => c.get('hello'))), chords.map(c => 'world!'))
})

test('stop', _ => chords.length * 3, function *(t) {
	while (chords.length) {
		var chord = chords.shift()
		chord.stop()

		console.log('node stopped, ' + chords.length + ' remainning')
		yield sleep(chord.hub.opts.peerCallTimeout * 1.5)

		var idToQuery = NodeId.create()
		t.ok((yield chords.map(c => c.node.findPredecessorId(idToQuery))).hasSameElements())
		t.ok((yield chords.map(c => c.node.findSuccessorId(idToQuery))).hasSameElements())
		t.deepEqual((yield chords.map(c => c.get('hello'))), chords.map(c => 'world!'))
	}

	wrtc.close()
})
