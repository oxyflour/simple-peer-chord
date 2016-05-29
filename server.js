const Chord = require('./lib/chord'),
	SocketIO = require('socket.io'),
	co = require('co'),
	http = require('http'),
	wrtc = require('wrtc'),

	server = http.Server(),
	ioServer = SocketIO(server),
	peerOptions = { wrtc },
	chord = new Chord({ peerOptions }, true),

	proxiedEvents = [
		'id',
		'signal'
	],
	conns = {
		// id -> sock
	}

ioServer.on('connection', sock => {
	sock.once('join', channel => {
		console.log('registered channel ' + channel)
		if (!channel) {
			sock.emit('joined', true)
			co(chord.connectWebsocket(sock))
		}
		else if (conns[channel]) {
			var target = conns[channel]
			delete conns[channel]

			sock.emit('joined', true)
			target.emit('joined', false)
			proxiedEvents.forEach(evt => {
				sock.on(evt, id => target.emit(evt, id))
				target.on(evt, id => sock.emit(evt, id))
			})
		}
		else {
			conns[channel] = sock
		}
	})
})

server.listen(8088, _ => {
	console.log('listening at port ' + server.address().port)
})