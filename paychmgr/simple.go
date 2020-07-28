package paychmgr

import (
	"bytes"
	"context"

	"github.com/filecoin-project/lotus/api"

	"github.com/filecoin-project/specs-actors/actors/abi/big"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	init_ "github.com/filecoin-project/specs-actors/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/actors/builtin/paych"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"

	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/types"
)

// TODO:
//
// Handle settle event
//  - Mark channel as settled (in store)
//  - Any subsequent add funds should go to a new channel
//  - Tests
//

type paychApi interface {
	StateWaitMsg(ctx context.Context, msg cid.Cid, confidence uint64) (*api.MsgLookup, error)
	MpoolPushMessage(ctx context.Context, msg *types.Message) (*types.SignedMessage, error)
}

// paychFundsRes is the response to a create channel or add funds request
type paychFundsRes struct {
	channel *address.Address
	mcid    *cid.Cid
	err     error
}

// ensureFundsReq is a request to create a channel or add funds to a channel
type ensureFundsReq struct {
	ctx        context.Context
	from       address.Address
	to         address.Address
	amt        types.BigInt
	onComplete func(res *paychFundsRes)
}

func (ca *channelAccessor) getPaych(ctx context.Context, from, to address.Address, ensureFree types.BigInt) (*address.Address, *cid.Cid, error) {
	// Add the request to ensure funds to a queue and wait for the result
	promise := ca.enqueue(&ensureFundsReq{ctx: ctx, from: from, to: to, amt: ensureFree})
	select {
	case res := <-promise:
		return res.channel, res.mcid, res.err
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (ca *channelAccessor) enqueue(task *ensureFundsReq) chan *paychFundsRes {
	promise := make(chan *paychFundsRes)
	task.onComplete = func(res *paychFundsRes) {
		select {
		case <-task.ctx.Done():
		case promise <- res:
		}
	}

	ca.lk.Lock()
	defer ca.lk.Unlock()

	ca.ensureFundsReqQueue = append(ca.ensureFundsReqQueue, task)
	go ca.checkQueue()

	return promise
}

func (ca *channelAccessor) checkQueue() {
	ca.lk.Lock()
	defer ca.lk.Unlock()

	if len(ca.ensureFundsReqQueue) == 0 {
		return
	}

	head := ca.ensureFundsReqQueue[0]
	res := ca.processTask(head.ctx, head.from, head.to, head.amt)

	// If the task is waiting on an external event (eg something to appear on
	// chain) it will return nil
	if res == nil {
		// Stop processing the ensureFundsReqQueue and wait. When the event occurs it will
		// call checkQueue() again
		return
	}

	// If there was an error, invoke the callback for the task and all
	// subsequent ensureFundsReqQueue tasks with an error
	if res.err != nil && res.err != context.Canceled {
		for _, task := range ca.ensureFundsReqQueue {
			task.onComplete(&paychFundsRes{err: res.err})
		}
		ca.ensureFundsReqQueue = nil
		return
	}

	// The task has finished processing so clean it up
	ca.ensureFundsReqQueue[0] = nil // allow GC of element
	ca.ensureFundsReqQueue = ca.ensureFundsReqQueue[1:]

	// Task completed so callback with its results
	head.onComplete(res)

	// Process the next task
	if len(ca.ensureFundsReqQueue) > 0 {
		go ca.checkQueue()
	}
}

// msgWaitComplete is called when the message for a previous task appears on
// chain, or there is an error. In the case of an error, all subsequent
// tasks in the ensureFundsReqQueue are completed with the error.
func (ca *channelAccessor) msgWaitComplete(err error) {
	ca.lk.Lock()
	defer ca.lk.Unlock()

	if len(ca.ensureFundsReqQueue) == 0 {
		return
	}

	// If there was an error, complete all subsequent ensureFundsReqQueue tasks with an error
	if err != nil {
		for _, task := range ca.ensureFundsReqQueue {
			task.onComplete(&paychFundsRes{err: err})
		}
		ca.ensureFundsReqQueue = nil
		return
	}

	go ca.checkQueue()
}

func (ca *channelAccessor) processTask(ctx context.Context, from address.Address, to address.Address, ensureFree types.BigInt) *paychFundsRes {
	// Note: It's ok if we get ErrChannelNotTracked. It just means we need to
	// create a channel.
	channelInfo, err := ca.store.OutboundByFromTo(from, to)
	if err != nil && err != ErrChannelNotTracked {
		return &paychFundsRes{err: err}
	}

	// If a channel has not yet been created, create one.
	// Note that if the previous attempt to create the channel failed because of a VM error
	// (eg not enough gas), both channelInfo.Channel and channelInfo.CreateMsg will be nil.
	if channelInfo == nil || channelInfo.Channel == nil && channelInfo.CreateMsg == nil {
		mcid, err := ca.createPaych(ctx, from, to, ensureFree)
		if err != nil {
			return &paychFundsRes{err: err}
		}

		return &paychFundsRes{mcid: mcid}
	}

	// If the create channel message has been sent but the channel hasn't
	// been created on chain yet
	if channelInfo.CreateMsg != nil {
		// If the amount in the channel will cover the requested amount,
		// there's no need to add more funds so just return the channel
		// create message CID
		if channelInfo.PendingAmount.GreaterThanEqual(ensureFree) {
			return &paychFundsRes{mcid: channelInfo.CreateMsg}
		}

		// Otherwise just wait for the channel to be created and try again
		return nil
	}

	// If the channel already has the requested amount, there's no
	// need to add any more, just return the channel address
	if channelInfo.Amount.GreaterThanEqual(ensureFree) {
		return &paychFundsRes{channel: channelInfo.Channel, mcid: channelInfo.CreateMsg}
	}

	// If an add funds message was sent to the chain
	if channelInfo.AddFundsMsg != nil {
		// If the amount in the pending add funds message covers the amount for
		// this request, there's no need to add more, just return the message
		// CID for the pending request
		if channelInfo.PendingAmount.GreaterThanEqual(ensureFree) {
			return &paychFundsRes{channel: channelInfo.Channel, mcid: channelInfo.AddFundsMsg}
		}

		// Otherwise wait for the add funds message to complete and try again
		return nil
	}

	// We need to add more funds, so send an add funds message to
	// cover the amount for this request
	mcid, err := ca.addFunds(ctx, from, to, ensureFree)
	return &paychFundsRes{channel: channelInfo.Channel, mcid: mcid, err: err}
}

// createPaych sends a message to create the channel and returns the message cid
func (ca *channelAccessor) createPaych(ctx context.Context, from, to address.Address, amt types.BigInt) (*cid.Cid, error) {
	params, aerr := actors.SerializeParams(&paych.ConstructorParams{From: from, To: to})
	if aerr != nil {
		return nil, aerr
	}

	enc, aerr := actors.SerializeParams(&init_.ExecParams{
		CodeCID:           builtin.PaymentChannelActorCodeID,
		ConstructorParams: params,
	})
	if aerr != nil {
		return nil, aerr
	}

	msg := &types.Message{
		To:       builtin.InitActorAddr,
		From:     from,
		Value:    amt,
		Method:   builtin.MethodsInit.Exec,
		Params:   enc,
		GasLimit: 0,
		GasPrice: types.NewInt(0),
	}

	smsg, err := ca.api.MpoolPushMessage(ctx, msg)
	if err != nil {
		return nil, xerrors.Errorf("initializing paych actor: %w", err)
	}
	mcid := smsg.Cid()

	ci := &ChannelInfo{
		Direction:     DirOutbound,
		NextLane:      0,
		Control:       from,
		Target:        to,
		CreateMsg:     &mcid,
		PendingAmount: amt,
	}

	if err := ca.store.putChannelInfo(ci); err != nil {
		log.Errorf("tracking channel: %s", err)
		return &mcid, err
	}

	go func() {
		err := ca.waitForPaychCreateMsg(from, to, mcid)
		ca.msgWaitComplete(err)
	}()
	return &mcid, nil
}

// waitForPaychCreateMsg waits for mcid to appear on chain and stores the robust address of the
// created payment channel
func (ca *channelAccessor) waitForPaychCreateMsg(from address.Address, to address.Address, mcid cid.Cid) error {
	mwait, err := ca.api.StateWaitMsg(ca.waitCtx, mcid, build.MessageConfidence)
	if err != nil {
		log.Errorf("wait msg: %w", err)
		return err
	}

	if mwait.Receipt.ExitCode != 0 {
		err := xerrors.Errorf("payment channel creation failed (exit code %d)", mwait.Receipt.ExitCode)
		log.Error(err)

		ca.lk.Lock()
		defer ca.lk.Unlock()

		ca.mutateChannelInfo(from, to, func(channelInfo *ChannelInfo) {
			channelInfo.PendingAmount = big.NewInt(0)
			channelInfo.CreateMsg = nil
		})

		return err
	}

	var decodedReturn init_.ExecReturn
	err = decodedReturn.UnmarshalCBOR(bytes.NewReader(mwait.Receipt.Return))
	if err != nil {
		log.Error(err)
		return err
	}

	ca.lk.Lock()
	defer ca.lk.Unlock()

	ca.mutateChannelInfo(from, to, func(channelInfo *ChannelInfo) {
		channelInfo.Channel = &decodedReturn.RobustAddress
		channelInfo.Amount = channelInfo.PendingAmount
		channelInfo.PendingAmount = big.NewInt(0)
		channelInfo.CreateMsg = nil
	})

	return nil
}

// addFunds sends a message to add funds to the channel and returns the message cid
func (ca *channelAccessor) addFunds(ctx context.Context, from address.Address, to address.Address, ensureFree types.BigInt) (*cid.Cid, error) {
	channelInfo, err := ca.store.OutboundByFromTo(from, to)
	if err != nil {
		return nil, err
	}

	amt := big.Sub(ensureFree, channelInfo.PendingAmount)

	msg := &types.Message{
		To:       *channelInfo.Channel,
		From:     channelInfo.Control,
		Value:    amt,
		Method:   0,
		GasLimit: 0,
		GasPrice: types.NewInt(0),
	}

	smsg, err := ca.api.MpoolPushMessage(ctx, msg)
	if err != nil {
		return nil, err
	}
	mcid := smsg.Cid()

	ca.mutateChannelInfo(from, to, func(ci *ChannelInfo) {
		ci.PendingAmount = ensureFree
		ci.AddFundsMsg = &mcid
	})

	go func() {
		err := ca.waitForAddFundsMsg(from, to, mcid)
		ca.msgWaitComplete(err)
	}()

	return &mcid, nil
}

// waitForAddFundsMsg waits for mcid to appear on chain and returns error, if any
func (ca *channelAccessor) waitForAddFundsMsg(from address.Address, to address.Address, mcid cid.Cid) error {
	mwait, err := ca.api.StateWaitMsg(ca.waitCtx, mcid, build.MessageConfidence)
	if err != nil {
		log.Error(err)
		return err
	}

	if mwait.Receipt.ExitCode != 0 {
		err := xerrors.Errorf("voucher channel creation failed: adding funds (exit code %d)", mwait.Receipt.ExitCode)
		log.Error(err)

		ca.lk.Lock()
		defer ca.lk.Unlock()

		ca.mutateChannelInfo(from, to, func(channelInfo *ChannelInfo) {
			channelInfo.PendingAmount = big.NewInt(0)
			channelInfo.AddFundsMsg = nil
		})

		return err
	}

	ca.lk.Lock()
	defer ca.lk.Unlock()

	ca.mutateChannelInfo(from, to, func(channelInfo *ChannelInfo) {
		channelInfo.Amount = channelInfo.PendingAmount
		channelInfo.PendingAmount = big.NewInt(0)
		channelInfo.AddFundsMsg = nil
	})

	return nil
}

func (ca *channelAccessor) mutateChannelInfo(from address.Address, to address.Address, mutate func(*ChannelInfo)) {
	channelInfo, err := ca.store.OutboundByFromTo(from, to)

	// If there's an error reading or writing to the store just log an error.
	// For now we're assuming it's unlikely to happen in practice.
	// Later we may want to implement a transactional approach, whereby
	// we record to the store that we're going to send a message, send
	// the message, and then record that the message was sent.
	if err != nil {
		log.Errorf("Error reading channel info from store: %s", err)
	}

	mutate(channelInfo)

	err = ca.store.putChannelInfo(channelInfo)
	if err != nil {
		log.Errorf("Error writing channel info to store: %s", err)
	}
}
