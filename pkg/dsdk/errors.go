package dsdk

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrValidation Sentinel error to indicate a structural validation error, e.g. a missing property on a JSON object
	ErrValidation = errors.New("validation error")
	// ErrConflict indicates an object conflict, e.g. when creating an object that already exists
	ErrConflict = errors.New("conflict")
	// ErrNotFound indicates that a certain object does not exist
	ErrNotFound = errors.New("not found")
	// ErrInvalidInput Sentinel error to indicate a wrong input, e.g. a string when a number was expected, or an empty string
	ErrInvalidInput = errors.New("invalid input")
)

// NewValidationError Helper to create new ValidationError
func NewValidationError(messages ...string) error {
	return fmt.Errorf("%w: %s", ErrValidation, strings.Join(messages, "; "))
}
