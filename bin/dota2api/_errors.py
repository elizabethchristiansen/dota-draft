class ServiceNotAvailable( Exception ):
	pass

class InvalidAuthKey( Exception ):
	pass

class RateLimitActive( Exception ):
	pass

class CouldNotInit( Exception ):
	pass

class OAPIError( Exception ):
	pass