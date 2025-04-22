package definition

import (
	"context"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
)

type BusinessesRepository interface {
	FindById(context.Context, string) (*model.Business, error)
}
