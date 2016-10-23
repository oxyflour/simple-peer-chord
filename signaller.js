#!/usr/bin/env node

const Chord = require('./lib/chord'),
	SocketIO = require('socket.io'),
	io = require('socket.io-client'),
	co = require('co'),
	http = require('http'),
	path = require('path'),
	url = require('url'),
	program = require('commander'),
	wrtc = require('electron-webrtc')({ headless: true }),
	packageJson = require(path.join(__dirname, 'package.json')),

	server = http.Server(),
	ioServer = SocketIO(server),
	peerOptions = { wrtc },
	chord = new Chord({ peerOptions }, true),
	peerHosts = { /* host -> time */ },
	time = Date.now()

program
	.version(packageJson.version)
	.option('-p, --port <port>', 'listen port, default 8088', parseFloat, 8088)
	.option('-P, --peer <url>', 'peer websocket url', host => peerHosts[host] = { time })
	.parse(process.argv)

chord.getKnownNodes = function() {
	var nodes = { }
	chord.node.fingerIds
		.concat(chord.node.succBackupIds)
		.concat(chord.node.predBackupIds)
		.concat(chord.node.predecessorId)
		.forEach(id => nodes[id] = true)
	return Object.keys(nodes)
}

ioServer.on('connection', sock => co(function *() {
	try {
		yield *chord.connectViaSocketIO(sock)
	}
	catch (err) {
		console.error('[ws]', err)
	}
}))

server.addListener('request', (req, res) => {
	var reqUrl = req.url.match(/^\w:\/\/.*/) ? 'http://' + req.headers.host + req.url : req.url,
		parse = url.parse(reqUrl, true)
	if (req.method === 'GET' && parse.path === '/status') {
		var id = chord.id,
			peers = Object.keys(peerHosts),
			fingers = chord.node.fingerIds,
			conns = Object.keys(chord.hub.conns)
		res.end(JSON.stringify({ id, peers, fingers, conns }))
	}
})

chord.on('pub-sig-peer', data => {
	var time = Date.now(),
		peers = peerHosts
	peerHosts = { }
	Object.keys(peers)
		.filter(host => peers[host].time > time - 5 * 60 * 1000)
		.forEach(host => peerHosts[host] = peers[host])

	var id = data.id,
		host = data.host
	if (id !== chord.id && host) {
		peerHosts[host] = { id, time }
	}
})

chord.on('pub-sig-join', data => {
	var shouldJoin = data.nodes.length > chord.getKnownNodes().length
	if (shouldJoin) {
		co(chord.node.join(data.id))
	}
	chord.send(data.id, 'pub-sig-join-' + chord.id, shouldJoin)
})

setInterval(_ => co(function *() {
	yield chord.subscribe('sig-peer')

	var time = Date.now(),
		hosts = Object.keys(peerHosts),
		host = hosts[Math.floor(Math.random() * hosts.length)],
		id = peerHosts[host] && peerHosts[host].id
	if (host && chord.hub.has(id)) {
		peerHosts[host] = { id, time }
		chord.publish('sig-peer', 'pub-sig-peer', { id, host })
	}
	else if (host) try {
		console.log('[sig] connecting to peer ' + host)

		var sock = io('ws://' + host, { transports: ['websocket'], reconnection: false })
		yield new Promise((resolve, reject) => {
			sock.on('connect', resolve)
			sock.on('connect_error', reject)
		})

		id = yield* chord.connectViaSocketIO(sock, true)
		if (id && id !== chord.id) {
			chord.send(id, 'pub-sig-join', { id: chord.id, nodes: chord.getKnownNodes() })

			var hasJoined = yield new Promise((resolve, reject) => {
				setTimeout(reject, 10000, 'join to ' + id + ' timeout')
				chord.once('pub-sig-join-' + id, resolve)
			})
			if (!hasJoined) {
				yield *chord.node.join(id)
			}

			peerHosts[host] = { id, time }
			chord.publish('sig-peer', 'pub-sig-peer', { id, host })
			console.log('[sig] joined to ' + id + ' at ' + host)
		}

		console.log('[sig] ' + host + ' connected')
	}
	catch (err) {
		console.error('[sig]', err)
	}
}), 10000)

server.listen(program.port, _ => {
	console.log('listening at port ' + server.address().port)
})
