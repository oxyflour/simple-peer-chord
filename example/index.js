const Chord = require('../lib/chord'),
	NodeId = require('../lib/node-id'),
	co = require('co')

const HOST = 't.ofr.me:8088',
	BOOTSTRAP = {
		url: 'ws://' + HOST,
		opts: { transports:['websocket'] },
	}

require('debug').enable('chord*')

global.chords = [ ]
global.co = co

global.checkRoute = function(button, target) {
	var id = NodeId.create(target),
		cs = global.chords.filter(c => Object.keys(c.hub.conns).length)

	button.disabled = true
	button.innerText = 'checking route...'
	co(function *() {
		try {
			var successorIds = yield cs.map(c => c.node.findSuccessorId(id)),
				predecessorIds = yield cs.map(c => c.node.findPredecessorId(id)),
				isOK = successorIds.every(id => id === successorIds[0]) &&
					predecessorIds.every(id => id === predecessorIds[0])
			button.innerText = 'check route ' + (isOK ? '(ok)' : '(not ok)')
			button.disabled = false
		}
		catch (err) {
			console.error(err)
			button.disabled = false
		}
	})
}

global.addChord = function(button, id) {
	var chord = new Chord({ id }, global.chords[0] || BOOTSTRAP || true)

	button.disabled = true
	chord.on('say', data => {
		console.log('[' + chord.id + ']', data)
	})
	chord.once('chord-start', _ => {
		button.disabled = false
		if (-- nodeCount > 0) {
			setTimeout(_ => global.addChord(button), 3000)
		}
	})

	global.chords.push(chord)
	global.chords['$' + chord.id] = chord
}

var script = document.createElement('script'),
	nodeCount = 15
script.onload = _ => global.addChord({ })
script.src = '//' + HOST + '/socket.io/socket.io.js'
document.body.appendChild(script)

const DIAMETER = 500,
	RADIUS = DIAMETER / 2

var bundle = d3.layout.bundle()

var cluster = d3.layout.cluster()
	.size([360, RADIUS - 120])
	.sort((a, b) => d3.ascending(parseInt(a.name, 16), parseInt(b.name, 16)))
	.value(d => d.size)

var svg = d3.select('body').append('svg')
	.attr('width', DIAMETER)
	.attr('height', DIAMETER)
	.append('g')
		.attr('transform', `translate(${RADIUS}, ${RADIUS})`)

var line = d3.svg.line.radial()
    .interpolate('bundle')
    .tension(.8)
    .radius(d => d.y)
    .angle(d => d.x / 180 * Math.PI)

var linksAlreadyShown = ''

setTimeout(function draw() {
	setTimeout(draw, 1000)

	var allNodes = global.chords.reduce((ls, chord) =>
			ls.concat(chord.id)
				.concat(chord.node.fingerIds)
				.concat(chord.node.succBackupIds)
				.concat(Object.keys(chord.hub.conns)), [ ])
	allNodes = allNodes.filter((id, i) => id && allNodes.indexOf(id) === i)

	var allLinks = { }, nodeConns = { }, nodeStore = { }
	global.chords.forEach((chord, i) => {
		nodeConns[chord.id] = Object.keys(chord.hub.conns).length
		nodeStore[chord.id] = Object.keys(chord.node.storage).length
		Object.keys(chord.hub.conns).forEach(id => {
			var key = [chord.id, id].sort().join(':')
			allLinks[key] = true
		})
	})

	var linksToShow = Object.keys(allLinks).sort().join(';')
	if (linksAlreadyShown !== linksToShow) {
		linksAlreadyShown = linksToShow

		var root = { }, map = { }
		var children = root.children = allNodes.map(id => map[id] = {
			name: id,
			size: 1,
			parent: root,
		})

		var nodes = cluster.nodes(root)
		var links = bundle(Object.keys(allLinks).map(key => key.split(':')).map(pair => ({
			source: map[ pair[0] ],
			target: map[ pair[1] ],
		})))

		svg.selectAll('*').remove()

		svg.selectAll('.link')
			.data(links)
			.enter()
				.append('path')
					.attr('class', 'link')
					.attr('d', line)

		svg.selectAll('.node')
			.data(nodes.filter(n => !n.children))
			.enter()
				.append('g')
					.attr('class', 'node')
					.attr('transform', d => `rotate(${d.x - 90}) translate(${d.y})`)
				.append('text')
					.attr('dx', d => d.x < 180 ? 8 : -8)
					.attr('dy', '.31em')
					.attr('text-anchor', d => d.x < 180 ? 'start' : 'end')
					.attr('transform', d => d.x < 180 ? null : 'rotate(180)')
					.text(d =>
						(d.name.length > 12 ? d.name.substr(0, 10) + '...' : d.name) +
						(nodeConns[d.name] ? ' [' + nodeConns[d.name] + ']' : ''))
					.classed('has-conn', d => nodeConns[d.name] >= 0)
	}

	svg.selectAll('.node text')
		.attr('fill', d => nodeStore[d.name] ? 'red' : 'black')

}, 1000)