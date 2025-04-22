package businesses

import (
	"errors"
	"fmt"
	"log"
	"math"
)

const (
	earthRadius              = 6371.0
	GeohashPrecision         = 10.0
	gEarthMeriCircumference  = 40007860.0
	gMetersPerDegreeLatitude = 110574.0
	gBitsPerChar             = 5.0
	gMaxBitsPrecision        = 22 * gBitsPerChar
	gEarthEqRadius           = 6378137.0
	gE2                      = 0.00669447819799
	milePerKm                = 0.621371
)

var (
	gBase32  = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "b", "c", "d", "e", "f", "g", "h", "j", "k", "m", "n", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}
	gEpsilon = math.Exp(-12)
)

/**
 * Validates the inputted location and throws an error if it is invalid.
 *
 * @param location The [latitude, longitude] pair to be verified.
 */
func validateLocation(location []float64) (bool, error) {
	var err error
	if len(location) != 2 {
		err = errors.New(fmt.Sprintf("expected array of length 2, got length %v", len(location)))
		return false, err
	}
	latitude := location[0]
	longitude := location[1]

	if latitude < -90 || latitude > 90 {
		err = errors.New("latitude must be within the range [-90, 90]")
		return false, err
	}
	if longitude < -180 || longitude > 180 {
		err = errors.New("longitude must be within the range [-180, 180]")
		return false, err
	}
	return true, nil
}

/**
 * Validates the inputted geohash and throws an error if it is invalid.
 *
 * @param geohash The geohash to be validated.
 */
func validateGeohash(geohash string) (bool, error) {
	var err error
	if len(geohash) == 0 {
		err = errors.New("geohash cannot be the empty string")
		return false, err
	}

	for _, letter := range []rune(geohash) {
		if indexOf(gBase32, string(letter)) == -1 {
			err = errors.New(fmt.Sprintf("geohash cannot contain '%v'", letter))
			return false, err
		}
	}
	return true, nil
}

func EncodeGeohash(location []float64, precision float64) string {
	hash := ""
	hashVal := 0
	bits := 0
	even := true
	longitudeRange := map[string]float64{"min": -180, "max": 180}
	latitudeRange := map[string]float64{"min": -90, "max": 90}

	for {
		if len(hash) < int(precision) {
			var val float64
			if even {
				val = location[1]
			} else {
				val = location[0]
			}

			var range_ map[string]float64
			if even {
				range_ = longitudeRange
			} else {
				range_ = latitudeRange
			}

			mid := (range_["min"] + range_["max"]) / 2

			if val > mid {
				hashVal = (hashVal << 1) + 1
				range_["min"] = mid
			} else {
				hashVal = (hashVal << 1) + 0
				range_["max"] = mid
			}

			even = !even
			if bits < 4 {
				bits++
			} else {
				bits = 0
				hash += gBase32[hashVal]
				hashVal = 0
			}
		} else {
			break
		}
	}

	return hash
}

/**
 * Calculates the bits necessary to reach a given resolution, in meters, for the longitude at a
 * given latitude.
 *
 * @param resolution The desired resolution.
 * @param latitude The latitude used in the conversion.
 * @return The bits necessary to reach a given resolution, in meters.
 */
func longitudeBitsForResolution(resolution float64, latitude float64) float64 {
	degs := metersToLongitudeDegrees(resolution, latitude)
	if math.Abs(degs) > 0.000001 {
		return math.Max(1, math.Log2(360/degs))
	}
	return 1
}

/**
 * Calculates the bits necessary to reach a given resolution, in meters, for the latitude.
 *
 * @param resolution The bits necessary to reach a given resolution, in meters.
 * @returns Bits necessary to reach a given resolution, in meters, for the latitude.
 */
func latitudeBitsForResolution(resolution float64) float64 {
	return math.Min(math.Log2(gEarthMeriCircumference/2/resolution), gMaxBitsPrecision)
}

/**
 * Wraps the longitude to [-180,180].
 *
 * @param longitude The longitude to wrap.
 * @returns longitude The resulting longitude.
 */
func wrapLongitude(longitude float64) float64 {
	if longitude <= 180 && longitude >= -180 {
		return longitude
	}
	adjusted := longitude + 180
	if adjusted > 0 {
		return math.Mod(adjusted, 360) - 180
	}
	return 180 - math.Mod(-adjusted, 360)
}

/**
 * Calculates the maximum number of bits of a geohash to get a bounding box that is larger than a
 * given size at the given coordinate.
 *
 * @param coordinate The coordinate as a [latitude, longitude] pair.
 * @param size The size of the bounding box.
 * @returns The number of bits necessary for the geohash.
 */
func boundingBoxBits(coordinate []float64, size float64) float64 {
	latDeltaDegrees := size / gMetersPerDegreeLatitude
	latitudeNorth := math.Min(90, coordinate[0]+latDeltaDegrees)
	latitudeSouth := math.Max(-90, coordinate[0]-latDeltaDegrees)
	bitsLat := math.Floor(latitudeBitsForResolution(size)) * 2
	bitsLongNorth := math.Floor(longitudeBitsForResolution(size, latitudeNorth))*2 - 1
	bitsLongSouth := math.Floor(longitudeBitsForResolution(size, latitudeSouth))*2 - 1
	return math.Min(bitsLat, math.Min(bitsLongNorth, math.Min(bitsLongSouth, gMaxBitsPrecision)))
}

func boundingBoxCoordinates(center []float64, radius float64) [][]float64 {
	latDegrees := radius / gMetersPerDegreeLatitude
	latitudeNorth := math.Min(90, center[0]+latDegrees)
	latitudeSouth := math.Max(-90, center[0]-latDegrees)
	longDegsNorth := metersToLongitudeDegrees(radius, latitudeNorth)
	longDegsSouth := metersToLongitudeDegrees(radius, latitudeSouth)
	longDegs := math.Max(longDegsNorth, longDegsSouth)
	return [][]float64{
		{center[0], center[1]},
		{center[0], wrapLongitude(center[1] - longDegs)},
		{center[0], wrapLongitude(center[1] + longDegs)},
		{latitudeNorth, center[1]},
		{latitudeNorth, wrapLongitude(center[1] - longDegs)},
		{latitudeNorth, wrapLongitude(center[1] + longDegs)},
		{latitudeSouth, center[1]},
		{latitudeSouth, wrapLongitude(center[1] - longDegs)},
		{latitudeSouth, wrapLongitude(center[1] + longDegs)},
	}
}

/**
 * Calculates the bounding box query for a geohash with x bits precision.
 *
 * @param geohash The geohash whose bounding box query to generate.
 * @param bits The number of bits of precision.
 * @returns A [start, end] pair of geohashes.
 */
func geohashQuery(geohash string, bits float64) []string {
	if ok, err := validateGeohash(geohash); !ok {
		log.Fatal(err)
	}
	precision := int(math.Ceil(bits / gBitsPerChar))
	if len(geohash) < precision {
		return []string{geohash, geohash + "~"}
	}
	geohash = string([]rune(geohash)[0:precision])
	base := string([]rune(geohash)[0 : len(geohash)-1])
	runeAt := []rune(geohash)[len(geohash)-1]
	lastValue := indexOf(gBase32, string(runeAt))
	significantBits := int(bits) - (len(base) * int(gBitsPerChar))
	unusedBits := int(gBitsPerChar) - significantBits
	// delete unused bits
	startValue := (lastValue >> unusedBits) << unusedBits
	endValue := startValue + (1 << unusedBits)
	if endValue > 31 {
		return []string{base + gBase32[startValue], base + "~"}
	}
	return []string{base + gBase32[startValue], base + gBase32[endValue]}
}

func indexOf(strings []string, runeAt string) int {
	for i, str := range strings {
		if str == runeAt {
			return i
		}
	}
	return 0
}

/**
 * Calculates a set of queries to fully contain a given circle. A query is a [start, end] pair
 * where any geohash is guaranteed to be lexiographically larger then start and smaller than end.
 *
 * @param center The center given as [latitude, longitude] pair.
 * @param radius The radius of the circle.
 * @return An array of geohashes containing a [start, end] pair.
 */
func geohashQueries(center []float64, radius float64) [][]string {
	if ok, err := validateLocation(center); !ok {
		log.Fatal(err)
	}
	queryBits := math.Max(1, boundingBoxBits(center, radius))
	geohashPrecision := math.Ceil(queryBits / gBitsPerChar)
	coordinates := boundingBoxCoordinates(center, radius)
	queries := Map(coordinates, geohashPrecision, queryBits)
	// remove duplicates
	return filterDuplicate(queries)
}

func filterDuplicate(queries [][]string) [][]string {
	queries2 := make([][]string, 0)
	for index, query := range queries {
		for otherIndex, other := range queries {
			if !(index > otherIndex && query[0] == other[0] && query[1] == other[1]) {
				queries2 = append(queries2, query)
				break
			}
		}
	}
	return queries2
}

func Map(coordinates [][]float64, precision float64, bits float64) [][]string {
	queries := make([][]string, len(coordinates))
	for i, coordinate := range coordinates {
		queries[i] = geohashQuery(EncodeGeohash(coordinate, precision), bits)
	}
	return queries
}

func metersToLongitudeDegrees(distance float64, latitude float64) float64 {
	radians := degreesToRadians(latitude)
	num := math.Cos(radians) * gEarthEqRadius * math.Pi / 180
	denom := 1 / math.Sqrt(1-gE2*math.Sin(radians)*math.Sin(radians))
	deltaDeg := num * denom
	if deltaDeg >= gEpsilon {
		return math.Min(360, distance/deltaDeg)
	}
	if distance > 0 {
		return 360
	}
	return 0
}

/**
 * Method which calculates the distance, in kilometers, between two locations,
 * via the Haversine formula. Note that this is approximate due to the fact that the
 * Earth's radius varies between 6356.752 km and 6378.137 km.
 *
 * @param location1 The [latitude, longitude] pair of the first location.
 * @param location2 The [latitude, longitude] pair of the second location.
 * @returns The distance, in kilometers, between the inputted locations.
 */
func distance(location1, location2 []float64) float64 {
	if ok, err := validateLocation(location1); !ok {
		log.Fatal(err)
	}
	if ok, err := validateLocation(location2); !ok {
		log.Fatal(err)
	}

	latDelta := degreesToRadians(location2[0] - location1[0])
	lonDelta := degreesToRadians(location2[1] - location1[1])

	a := (math.Sin(latDelta/2) * math.Sin(latDelta/2)) +
		(math.Cos(degreesToRadians(location1[0])) * math.Cos(degreesToRadians(location2[0])) *
			math.Sin(lonDelta/2) * math.Sin(lonDelta/2))

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadius * c
}

func degreesToRadians(degrees float64) float64 {
	return degrees * math.Pi / 180
}
