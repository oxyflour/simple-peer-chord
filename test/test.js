var test = require('tape'),
	co = require('co'),
	Chord = require('../lib/chord'),
	NodeId = require('../lib/node-id')()

var count = 5,
	chords = [ ],
	interval = 3000,
	opts = {
		nodePollTimeout: 300,
		nodeOptions: {
			fixFingerCocurrency: 64,
		},
		hubOptions: {
		},
	}

global.chords = chords

test('create a node', t => {
	t.plan(4)

	chords[0] = new Chord(opts, true)

	setTimeout(_ => {
		var idToQuery = NodeId.create()
		Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
			.then(ret => t.deepEqual(ret, chords.map(c => ret[0])))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
			.then(ret => t.deepEqual(ret, chords.map(c => ret[0])))
		Promise.all(chords.map(c => co(c.node.findPredecessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.node.predecessorId)))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(c.id))))
			.then(ret => t.deepEqual(ret, chords.map(c => c.id)))
	}, 500)
})

test('join to network', t => {
	t.plan(4 * count)

	var remaining = count

	function addChord() {
		var chord = new Chord(opts, chords[0])
		chords.push(chord)

		chord.on('info', data => console.info(data))

		console.log('node added, ' + (-- remaining) + ' remainning')
		chord.once('chord-start', _ => setTimeout(_ => checkResult(_ => {
			if (remaining > 0)
				setTimeout(addChord, 500)
		}), interval))
	}

	function checkResult(callback) {
		var idToQuery = NodeId.create()
		return Promise.all([
			Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
				.then(ret => t.deepEqual(ret, chords.map(c => ret[0]))),
			Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
				.then(ret => t.deepEqual(ret, chords.map(c => ret[0]))),
			Promise.all(chords.map(c => co(c.node.findPredecessorId(c.id))))
				.then(ret => t.deepEqual(ret, chords.map(c => c.node.predecessorId))),
			Promise.all(chords.map(c => co(c.node.findSuccessorId(c.id))))
				.then(ret => t.deepEqual(ret, chords.map(c => c.id))),
		]).then(callback)
	}

	addChord()
})

test('routing', t => {
	t.plan(chords.length * (chords.length - 1))

	chords.forEach(c => c.on('test-routing', data => {
		t.equal(data, c.id)
	}))

	chords.forEach(c => chords.forEach(s => {
		if (c !== s) s.send(c.id, 'test-routing', c.id)
	}))
})

test('multi-routing', t => {
	t.plan(chords.length)

	var rand = Math.random()

	chords.forEach(c => c.on('test-multi-routing', data => {
		t.equal(data, rand)
	}))

	chords[0].send(chords.map(c => c.id), 'test-multi-routing', rand)
})

test('put/get #1', t => {
	t.plan(1)

	chords[0].put('hello', 'world')

	setTimeout(_ => {
		Promise.all(chords.map(c => c.get('hello')))
			.then(ret => t.deepEqual(ret, chords.map(c => 'world')))
	}, interval)
})

test('put/get #2', t => {
	t.plan(1)

	chords[0].put('hello', 'world!')

	setTimeout(_ => {
		Promise.all(chords.map(c => c.get('hello')))
			.then(ret => t.deepEqual(ret, chords.map(c => 'world!')))
	}, interval)
})

test('subscribe/publish #1', t => {
	t.plan(3 * (chords.length - 1))

	var cs = chords.slice(0, 3),
		channel = '1'

	Promise.all(cs.map(c => c.subscribe(channel))).then(_ => {
		chords.forEach(c => c.on('test-sub-pub', ch => t.equal(ch, channel)))

		setTimeout(_ => {
			chords.forEach(c => c.publish(channel, 'test-sub-pub', channel))
		}, interval)
	})
})

test('subscribe/publish #2', t => {
	t.plan(2 * (chords.length - 1))

	var cs = chords.slice(0, 3),
		channel = '1'

	cs[0].unsubscribe(channel).then(_ => {
		chords.forEach(c => c.on('test-sub-pub2', ch => t.equal(ch, channel)))

		setTimeout(_ => {
			chords.forEach(c => c.publish(channel, 'test-sub-pub2', channel))
		}, interval)
	})
})

test('node failure', t => {
	t.plan(3)

	var keys = chords.map(c => Object.keys(c.node.storage)).filter(keys => keys[0]),
		chord = chords.filter(c => c.node.isResponsibleFor(keys[0]))[0]
	chord.stop()
	chords.splice(chords.indexOf(chord), 1)

	setTimeout(_ => {
		var idToQuery = NodeId.create()
		Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
			.then(ret => t.deepEqual(ret, chords.map(c => ret[0])))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
			.then(ret => t.deepEqual(ret, chords.map(c => ret[0])))
		Promise.all(chords.map(c => c.get('hello')))
			.then(ret => t.deepEqual(ret, chords.map(c => 'world!')))
	}, interval)
})

test('node rejoin', t => {
	t.plan(3)

	var chord = new Chord(opts, chords[0])
	chords.push(chord)

	chord.once('chord-start', _ => setTimeout(_ => {
		var idToQuery = NodeId.create()
		Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
			.then(ret => t.deepEqual(ret, chords.map(c => ret[0])))
		Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
			.then(ret => t.deepEqual(ret, chords.map(c => ret[0])))
		Promise.all(chords.map(c => c.get('hello')))
			.then(ret => t.deepEqual(ret, chords.map(c => 'world!')))
	}, interval))
})

test('stop', t => {
	t.plan(chords.length * 3)

	var stopChord = function() {
		chords.shift().stop()
		setTimeout(_ => checkResult(_ => {
			if (chords.length)
				setTimeout(stopChord, 1000)
		}), interval)
	}

	function checkResult(callback) {
		var idToQuery = NodeId.create()
		Promise.all([
			Promise.all(chords.map(c => co(c.node.findPredecessorId(idToQuery))))
				.then(ret => t.deepEqual(ret, chords.map(c => ret[0]))),
			Promise.all(chords.map(c => co(c.node.findSuccessorId(idToQuery))))
				.then(ret => t.deepEqual(ret, chords.map(c => ret[0]))),
			Promise.all(chords.map(c => c.get('hello')))
				.then(ret => t.deepEqual(ret, chords.map(c => 'world!'))),
		]).then(callback)
	}

	stopChord()
})
