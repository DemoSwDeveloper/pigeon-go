package data

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
)

// todo: add Context parameter to repo methods
type AppointmentsRepository interface {
	Delete(data *model.Appointment) (string, error)
	Update(data *model.Appointment) (string, error)
	Save(data *model.Appointment) (*model.Appointment, error)
	FindById(businessId, appointId string) (*model.Appointment, error)
	DeleteById(businessId, appointId string) (string, error)
}

type appointmentsRepository struct {
	db *firestore.Client
}

func NewAppointmentsRepo(client *firestore.Client) AppointmentsRepository {
	return appointmentsRepository{db: client}
}

func (r appointmentsRepository) Save(data *model.Appointment) (*model.Appointment, error) {
	doc, _, err := r.db.Collection("businesses").Doc(data.Business.Id).Collection("appointments").Add(context.Background(), data)
	if err != nil {
		return nil, err
	}
	data.Id = doc.ID
	return data, nil
}

func (r appointmentsRepository) FindById(businessId, appointId string) (*model.Appointment, error) {
	snapshot, err := r.db.Collection("businesses").Doc(businessId).Collection("appointments").Doc(appointId).Get(context.Background())
	if err != nil {
		return nil, err
	}
	var appoint *model.Appointment
	err = snapshot.DataTo(&appoint)
	if err != nil {
		return nil, err
	}
	appoint.Id = snapshot.Ref.ID
	return appoint, nil
}

func (r appointmentsRepository) Delete(data *model.Appointment) (string, error) {
	return r.DeleteById(data.Business.Id, data.Id)
}

func (r appointmentsRepository) DeleteById(businessId, appointId string) (string, error) {
	_, err := r.db.Collection("businesses").Doc(businessId).Collection("appointments").Doc(appointId).Delete(context.Background())
	if err != nil {
		return appointId, err
	}
	return appointId, nil
}

func (r appointmentsRepository) Update(data *model.Appointment) (string, error) {
	businessId := data.Business.Id
	id := data.Id
	_, err := r.db.Collection("businesses").Doc(businessId).Collection("appointments").Doc(id).Set(context.Background(), &data)
	return id, err
}
