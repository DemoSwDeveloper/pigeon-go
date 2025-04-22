package definition

type CustomersService interface {
	BlockCustomer(req BlockCustomerRequest) BlockCustomerResponse
	UnblockCustomer(req BlockCustomerRequest) BlockCustomerResponse
}

type BlockCustomerRequest struct {
	BusinessId  string
	CustomerId  string
	AssociateId string
}

type BlockCustomerResponse struct {
	Result bool
	Error  error
}
