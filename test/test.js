var test = require('tape'),
	co = require('co'),
	wrtc = require('wrtc'),
	Chord = require('../lib/chord'),
	NodeId = require('../lib/node-id')()

var count = 8,
	chords = [ ],
	interval = 3000,
	opts = {
		stabilizeInterval: 500,
		peerOptions: { wrtc },
	}

test('create a node', t => {
	t.plan(4)

	var chord = chords[0] = new Chord(opts, true)

	setTimeout(_ => {
		co(chord.node.findPredecessorId(chord.id))
			.then(id => t.equal(id, chord.id))
		co(chord.node.findSuccessorId(chord.id))
			.then(id => t.equal(id, chord.id))
		co(chord.node.findPredecessorId(NodeId.create()))
			.then(id => t.equal(id, chord.id))
		co(chord.node.findSuccessorId(NodeId.create()))
			.then(id => t.equal(id, chord.id))
	}, 1000)
})

test('join to network', t => {
	t.plan(4)

	var remaining = count

	function addChord() {
		var chord = new Chord(opts, chords[0])
		chords.push(chord)

		console.log('node added, ' + (-- remaining) + ' remainning')
		chord.recv(evt => {
			if (evt === 'chord-start')
				return setTimeout(remaining > 0 ? addChord : checkResult, interval)
		})
	}

	function checkResult() {
		var idToQuery = NodeId.create()
		Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findPredecessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.node.predecessorId)))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.id)))
	}

	addChord()
})

test('routing', t => {
	t.plan(count * (count + 1))

	chords.forEach(c => c.recv((evt, data) => {
		if (evt === 'ping')
			t.equal(data, c.id)
	}))

	chords.forEach(c => chords.forEach(s => {
		if (c !== s)
			s.send(c.id, 'ping', c.id)
	}))
})

test('node failure', t => {
	t.plan(4)

	chords.pop().stop()

	setTimeout(_ => {
		var idToQuery = NodeId.create()
		Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findPredecessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.node.predecessorId)))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.id)))
	}, interval)
})

test('node rejoin', t => {
	t.plan(4)

	chords.push(new Chord(opts, chords[0]))

	setTimeout(_ => {
		var idToQuery = NodeId.create()
		Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
			.then(ret => t.ok(ret.every(r => r === ret[0])))
		Promise.all(chords.map(c => co(c.node.findPredecessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.node.predecessorId)))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.id)))
	}, interval)

	t.once('end', _ => chords.forEach(c => c.stop()))
})
