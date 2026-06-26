package config

// CursorConfig defines the opaque pagination cursor settings.
type CursorConfig struct {
	Secret string `env:"SECRET,required"`
}
