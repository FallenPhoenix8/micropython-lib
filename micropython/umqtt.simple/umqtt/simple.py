def publish(self, topic, msg, retain=False, qos=0):
    print(f"Preparing to publish: topic={topic}, msg={msg}, retain={retain}, qos={qos}")

    # Encode the topic and message in UTF-8
    topic = topic.encode('utf-8')
    msg = msg.encode('utf-8')

    # Calculate the size of the message
    sz = 2 + len(topic) + len(msg)
    if qos > 0:
        sz += 2

    assert sz < 2097152  # MQTT supports a maximum of 2MB messages
    print(f"Calculated message size: {sz}")

    # Create the packet header
    pkt = bytearray(5)  # Header can be up to 5 bytes
    pkt[0] = 0x30 | (qos << 1) | retain  # Message type (PUBLISH)
    i = 1
    while sz > 0x7F:  # Multi-byte length encoding
        pkt[i] = (sz & 0x7F) | 0x80
        sz >>= 7
        i += 1
    pkt[i] = sz

    # Send the header and data
    self.sock.write(pkt[:i + 1])  # Header
    self._send_str(topic)  # Topic
    self.sock.write(msg)  # Message
    print(f"Message sent: {msg.decode('utf-8')}")

    # QoS handling
    if qos == 1:
        while True:
            op = self.wait_msg()
            if op == 0x40:
                sz = self.sock.read(1)
                assert sz == b'\x02'
                rcv_pid = self.sock.read(2)
                rcv_pid = rcv_pid[0] << 8 | rcv_pid[1]
                if self.pid == rcv_pid:
                    return
    elif qos == 2:
        raise NotImplementedError("QoS level 2 not implemented")
