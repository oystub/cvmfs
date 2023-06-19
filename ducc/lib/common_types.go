package lib

type OptionalBool int

const INVALID_ID int64 = -1

const (
	OB_DEFAULT OptionalBool = iota
	OB_TRUE
	OB_FALSE
)

func (ob OptionalBool) String() string {
	switch ob {
	case OB_DEFAULT:
		return "default"
	case OB_TRUE:
		return "true"
	case OB_FALSE:
		return "false"
	default:
		return "invalid"
	}
}

func (ob OptionalBool) MarshalJSON() ([]byte, error) {
	return []byte("\"" + ob.String() + "\""), nil
}

func (ob *OptionalBool) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case "default":
		*ob = OB_DEFAULT
	case "true":
		*ob = OB_TRUE
	case "false":
		*ob = OB_FALSE
	default:
		*ob = OB_DEFAULT
	}
	return nil
}
