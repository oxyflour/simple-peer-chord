var test = require('tape'),
	co = require('co'),
	wrtc = require('wrtc'),
	Chord = require('../lib/chord'),
	NodeId = require('../lib/node-id')

var count = 8,
	chords = [ ],
	opts = {
		stabilizeInterval: 200,
		signalTimeout: 1000,
		waitEventTimeout: 500,
		peerOptions: { wrtc },
	}

test('create a node', t => {
	t.plan(4)

	var chord = new Chord(opts)
	chord.start()

	setTimeout(_ => {
		co(chord.node.findPredecessorId(chord.id))
			.then(id => t.equal(id, chord.id))
		co(chord.node.findSuccessorId(chord.id))
			.then(id => t.equal(id, chord.id))
		co(chord.node.findPredecessorId(NodeId.from()))
			.then(id => t.equal(id, chord.id))
		co(chord.node.findSuccessorId(NodeId.from()))
			.then(id => t.equal(id, chord.id))
	}, 1000)

	t.once('end', _ => chord.stop())
})

test('join to network', t => {
	t.plan(4)

	var interval = 2000

	Array(count).fill(0).map((_, i) => setTimeout(_ => {
		var localChord = chords[0],
			chord = chords[i] = new Chord(opts)
		chord.start({ localChord })
		console.log('started chord #' + i)
	}, i * interval))

	setTimeout(_ => {
		var idToQuery = NodeId.from()
		Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findPredecessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.node.predecessorId)))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.id)))
	}, count * interval + 10000)
})

test('routing', t => {
	t.plan(count * (count - 1))

	chords.forEach(c => c.recv((evt, data) => {
		if (evt === 'ping')
			t.equal(data, c.id)
	}))

	chords.forEach(c => chords.forEach(s => {
		if (c !== s)
			s.send(c.id, 'ping', c.id)
	}))
})

test('node failure #1', t => {
	t.plan(4)

	chords.pop().stop()

	setTimeout(_ => {
		var idToQuery = NodeId.from()
		Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findPredecessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.node.predecessorId)))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.id)))
	}, 10000)
})

test('node failure #2', t => {
	t.plan(4)

	chords.pop().stop()

	setTimeout(_ => {
		var idToQuery = NodeId.from()
		Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findPredecessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.node.predecessorId)))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.id)))
	}, 10000)
})

test('stop', t => {
	t.plan(1)

	chords.forEach(c => c.stop())

	t.ok(chords.every(c => !c.started))
})