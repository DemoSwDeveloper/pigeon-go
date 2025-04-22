package scheduler

import (
	"context"
	"log"
	"os"
	"sync"
	"time"
)

type jobScheduler struct {
	logger                  *log.Logger
	wg                      *sync.WaitGroup
	mappedCancellationsSync *sync.Map
	cancellations           []context.CancelFunc
}

type job struct {
	id   interface{} // jod ID
	task func(ctx context.Context)
}

type Scheduler interface {
	NewJob(id interface{}, task func(ctx context.Context)) *job
	AddPeriodic(ctx context.Context, job *job, interval time.Duration)
	AddOneShot(ctx context.Context, job *job, duration time.Duration)
	Cancel(jobId interface{})
	Clear(jobId interface{})
	Stop()
}

type Processor interface {
	processPeriodic(ctx context.Context, job *job, interval time.Duration)
	processOneShot(ctx context.Context, job *job, duration time.Duration)
}

func (js *jobScheduler) AddPeriodic(ctx context.Context, job *job, interval time.Duration) {
	ctx, cancel := context.WithCancel(ctx)
	js.cancellations = append(js.cancellations, cancel)
	js.logger.Println("added cancellation")

	js.wg.Add(1)
	go js.processPeriodic(ctx, job, interval)
}

func (js *jobScheduler) AddOneShot(ctx context.Context, job *job, duration time.Duration) {
	ctx, cancel := context.WithCancel(ctx)
	js.cancellations = append(js.cancellations, cancel)
	js.mappedCancellationsSync.Store(job.id, cancel)
	js.logger.Println("added one shot jod. ID:", job.id)

	js.wg.Add(1)
	go js.processOneShot(ctx, job, duration)
}

func (js *jobScheduler) Cancel(jobId interface{}) {
	if cancelFunc, ok := js.mappedCancellationsSync.LoadAndDelete(jobId); ok {
		js.logger.Println("stopping job. ID:", jobId)
		if cancel, ok := cancelFunc.(context.CancelFunc); ok {
			cancel()
			js.logger.Println("stopped job. ID:", jobId)
		}
	}
}

func (js *jobScheduler) Clear(jobId interface{}) {
	js.logger.Println("clearing job. ID:", jobId)
	js.mappedCancellationsSync.Delete(jobId)
	js.logger.Println("cleared job. ID:", jobId)
}

func (js *jobScheduler) Stop() {
	js.logger.Println("stopping all the contexts")
	for _, cancel := range js.cancellations {
		cancel()
	}
	js.cancellations = make([]context.CancelFunc, 0)
	js.mappedCancellationsSync = new(sync.Map)
	js.wg.Wait()
	js.logger.Println("contexts have been stopped")
}

func (js *jobScheduler) processPeriodic(ctx context.Context, job *job, interval time.Duration) {
	js.logger.Println("started ticker")
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			job.task(ctx)
		case <-ctx.Done():
			ticker.Stop()
			js.wg.Done()
			js.logger.Println("ticker context was closed")
			return
		}
	}
}

func (js *jobScheduler) processOneShot(ctx context.Context, job *job, duration time.Duration) {
	defer js.wg.Done()
	timer := time.NewTimer(duration)
	js.logger.Println("started job timer. ID:", job.id)
	for {
		select {
		case <-timer.C:
			job.task(ctx)
			js.logger.Println("job done. ID:", job.id)
			return
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			js.logger.Println("job canceled. context canceled. ID:", job.id)
			return
		}
	}
}

func (js *jobScheduler) NewJob(id interface{}, task func(ctx context.Context)) *job {
	js.logger.Println("created job:", id)
	return &job{id, task}
}

func New(logger *log.Logger) Scheduler {
	if logger == nil {
		logger = log.New(os.Stdout, "SCHED", log.LstdFlags)
	}
	return &jobScheduler{
		logger:                  logger,
		wg:                      new(sync.WaitGroup),
		mappedCancellationsSync: new(sync.Map),
		cancellations:           make([]context.CancelFunc, 0),
	}
}
