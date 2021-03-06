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
	nodeOptions = {
		fingerTableSize: 160,
		succListSize: 160,
		fixFingerCocurrency: 40,
	},
	chordOptions = {
		peerOptions,
		nodeOptions,
		minHubConnCount: 5,
		maxHubConnCount: 160,
		minNodeFingerSize: 20,
		maxNodeFingerSize: 160,
	},
	chord = new Chord(chordOptions, true),
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
		sock.disconnect()
	}
	catch (err) {
		console.error('[ws]', err)
		sock.disconnect()
	}
}))

server.addListener('request', (req, res) => {
	var reqUrl = req.url.match(/^\w:\/\/.*/) ? 'http://' + req.headers.host + req.url : req.url,
		parse = url.parse(reqUrl, true)
	if (req.method === 'GET' && parse.path === '/status') {
		var id = chord.id,
			peers = Object.keys(peerHosts),
			predecessorId = chord.node.predecessorId,
			fingers = chord.node.fingerIds,
			conns = Object.keys(chord.hub.conns)
		res.end(JSON.stringify({ id, peers, predecessorId, fingers, conns }))
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

co(function *() {
	while (1) {
		yield new Promise(resolve => setTimeout(resolve, 10000))
		chord.subscribe('sig-peer')

		var time = Date.now(),
			hosts = Object.keys(peerHosts),
			host = hosts[Math.floor(Math.random() * hosts.length)],
			id = peerHosts[host] && peerHosts[host].id,
			sock = null
		if (host && chord.hub.has(id)) {
			peerHosts[host] = { id, time }
			chord.publish('sig-peer', 'pub-sig-peer', { id, host })
		}
		else if (host) try {
			console.log('[sig] connecting to peer ' + host)

			sock = io('ws://' + host, { transports: ['websocket'], reconnection: false })
			yield new Promise((resolve, reject) => {
				sock.on('connect', resolve)
				sock.on('connect_error', reject)
				setTimeout(reject, 1000, 'websocket connection timeout')
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
				yield chord.publish('sig-peer', 'pub-sig-peer', { id, host })
				console.log('[sig] joined to ' + id + ' at ' + host)
			}

			console.log('[sig] ' + host + ' connected')
			sock.disconnect()
		}
		catch (err) {
			console.error('[sig]', err)
			sock && sock.disconnect()
		}
	}
})

server.listen(program.port, _ => {
	console.log('listening at port ' + server.address().port)
})
