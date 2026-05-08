package opcat

import (
	"bytes"
	"fmt"
	"log"
	"manindexer/common"
	"manindexer/pin"
	"strings"
	"time"

	"github.com/btcsuite/btcd/wire"
	zmq "github.com/pebbe/zmq4"
)

func (indexer *Indexer) ZmqHashblock() {}

func (indexer *Indexer) ZmqRun(chanMsg chan pin.MempollChanMsg) {
	context, _ := zmq.NewContext()
	subscriber, _ := context.NewSocket(zmq.SUB)
	defer subscriber.Close()
	subscriber.SetSubscribe("rawtx")
	err := subscriber.SetTcpKeepalive(1)
	if err != nil {
		log.Println("SetTcpKeepalive err,", err)
	}
	err = subscriber.SetTcpKeepaliveIdle(60)
	if err != nil {
		log.Println("SetTcpKeepaliveIdle err,", err)
	}
	err = subscriber.SetTcpKeepaliveIntvl(1)
	if err != nil {
		log.Println("SetTcpKeepaliveIntvl err,", err)
	}
	subscriber.SetRcvhwm(20000)
	subscriber.SetRcvbuf(1024 * 200)
	err = subscriber.Connect(common.Config.Opcat.ZmqHost)
	if err != nil {
		log.Println("Connect to OPCAT ZMQ error", err)
		return
	} else {
		log.Println("OPCAT ZMQ connected")
	}

	for {
		recvmsg, err := subscriber.Recv(0)
		if err != nil {
			log.Println("OPCAT ZMQ RecvMessage Err,", err)
			continue
		} else {
			if recvmsg == "rawtx" || len(recvmsg) < 10 {
				continue
			}
			var msgTx wire.MsgTx
			if err := msgTx.Deserialize(bytes.NewReader([]byte(recvmsg))); err != nil {
				continue
			}
			pinInscriptions := indexer.CatchPinsByTx(&msgTx, 0, 0, "", "", 0)
			if len(pinInscriptions) > 0 {
				chanMsg <- pin.MempollChanMsg{PinList: pinInscriptions, Tx: msgTx}
			}
			tansferList, err := indexer.TransferCheck(&msgTx)
			if err == nil && len(tansferList) > 0 {
				chanMsg <- pin.MempollChanMsg{PinList: tansferList, Tx: msgTx}
			}
		}
	}
}

func (indexer *Indexer) TransferCheck(tx *wire.MsgTx) (transferPinList []*pin.PinInscription, err error) {
	var outputList []string
	for _, in := range tx.TxIn {
		output := fmt.Sprintf("%s:%d", in.PreviousOutPoint.Hash.String(), in.PreviousOutPoint.Index)
		outputList = append(outputList, output)
	}
	pinList, err := (*indexer.DbAdapter).GetPinListByOutPutList(outputList)
	if err != nil {
		return
	}
	timeNow := time.Now().Unix()
	for _, pinNode := range pinList {
		arr := strings.Split(pinNode.Output, ":")
		if len(arr) < 2 {
			continue
		}
		transferPin := pin.PinInscription{
			Id:                 pinNode.Id,
			CreateAddress:      pinNode.Address,
			Timestamp:          timeNow,
			GenesisTransaction: tx.TxHash().String(),
			IsTransfered:       true,
		}
		info, err := indexer.GetOWnerAddress(pinNode.Output, tx)
		if err != nil {
			continue
		}
		transferPin.Address = info.Address
		transferPinList = append(transferPinList, &transferPin)
	}
	return
}
