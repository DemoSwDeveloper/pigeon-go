package definition

import (
	"context"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
)

type BusinessSettingsRepository interface {
	FindById(context.Context, string) (*model.Settings, error)
}
