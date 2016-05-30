const Chord = require('../lib/chord'),
	NodeId = require('../lib/node-id')(),
	co = require('co')

const HOST = 'u.ofr.me:8088',
	WEBSOCKET_BOOTSTRAP = {
		url: 'ws://' + HOST,
		opts: { transports:['websocket'] }
	}

global.chords = [ ]

global.checkRoute = function(button) {
	var id = NodeId.create(),
		cs = global.chords.filter(c => c.started && Object.keys(c.hub.conns).length)

	button.disabled = true
	button.innerText = 'checking route...'
	Promise.all([
		Promise.all(cs.map(c => co(c.node.findSuccessorId(id))))
			.then(r => (console.info(r), r.every(v => v === r[0]))),
		Promise.all(cs.map(c => co(c.node.findPredecessorId(id))))
			.then(r => (console.info(r), r.every(v => v === r[0]))),
	])
		.then(r => button.innerText = r[0] && r[1] ? 'check route (ok)' : 'check route (not ok)')
		.then(_ => button.disabled = false)
		.catch(_ => button.disabled = false)
}

global.addChord = function(button) {
	var bootstrap = global.chords.filter(c => c.started)[0] || true,
		chord = new Chord({ }, bootstrap)

	global.chords.push(chord)
	global.chords['$' + chord.id] = chord

	button.disabled = true
	chord.once('chord-start', _ => button.disabled = false)
}

global.acceptConn = function(button) {
	var bootstrap = global.chords.filter(c => c.started)[0],
		channel = 'some sample channel'

	button.disabled = true
	bootstrap.start(Object.assign(WEBSOCKET_BOOTSTRAP, { channel }))
		.then(_ => button.disabled = false)
		.catch(_ => button.disabled = false)
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

	var allLinks = { }, nodeConns = { }
	global.chords.forEach((chord, i) => {
		var ids = Object.keys(chord.hub.conns)
		nodeConns[chord.id] = ids.length
		ids.forEach(id => {
			var key = [chord.id, id].sort().join(':')
			allLinks[key] = true
		})
	})

	var linksToShow = Object.keys(allLinks).sort().join(';')
	if (linksAlreadyShown === linksToShow)
		return
	else
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
				.text(d => d.name +
					(nodeConns[d.name] ? ' [' + nodeConns[d.name] + ']' : ''))
				.classed('highlight', d => nodeConns[d.name] >= 0)
}, 2000)