package dtmrpcx

func FromDtmError(r interface{}) error {
	return r.(error)
}
