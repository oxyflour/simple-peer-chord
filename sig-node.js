const Chord = require('./lib/chord'),
	SocketIO = require('socket.io'),
	co = require('co'),
	http = require('http'),
	wrtc = require('electron-webrtc')({ headless: true }),

	server = http.Server(),
	ioServer = SocketIO(server),
	peerOptions = { wrtc },
	chord = new Chord({ peerOptions }, true)

ioServer.on('connection', sock => {
	co(chord.connectViaWebSocket(sock))
})

server.addListener('request', (req, res) => {
	if (req.method === 'GET' && req.url === '/status') res.end(JSON.stringify({
		id: chord.id,
	}))
})

server.listen(8088, _ => {
	console.log('listening at port ' + server.address().port)
})
