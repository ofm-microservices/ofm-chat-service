package config

// CryptoConfig defines message text encryption settings.
type CryptoConfig struct {
	Secret string `env:"SECRET,required"`
}
