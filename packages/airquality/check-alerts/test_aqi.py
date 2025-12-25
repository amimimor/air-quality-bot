"""
Unit tests for AQI calculation functions.

Run with: python -m pytest test_aqi.py -v
"""
import pytest
import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys
import importlib.util

# Load the __main__.py module directly
spec = importlib.util.spec_from_file_location(
    "check_alerts",
    Path(__file__).parent / "__main__.py"
)
check_alerts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_alerts)

# Import functions from the loaded module
calculate_sub_index = check_alerts.calculate_sub_index
calculate_aqi = check_alerts.calculate_aqi
get_alert_level = check_alerts.get_alert_level
get_benzene_level = check_alerts.get_benzene_level
should_alert = check_alerts.should_alert
get_cached_reading = check_alerts.get_cached_reading
set_cached_reading = check_alerts.set_cached_reading
READINGS_CACHE_TTL = check_alerts.READINGS_CACHE_TTL
BREAKPOINTS = check_alerts.BREAKPOINTS
ALERT_LEVELS = check_alerts.ALERT_LEVELS
BENZENE_THRESHOLDS = check_alerts.BENZENE_THRESHOLDS


class TestCalculateSubIndex:
    """Tests for calculate_sub_index function."""

    def test_pm25_good_range(self):
        """PM2.5 in good range (0-18.5 ug/m3) should give sub-index 0-49."""
        breakpoints = BREAKPOINTS["PM2.5"]
        assert calculate_sub_index(0, breakpoints) == 0
        assert calculate_sub_index(10, breakpoints) == pytest.approx(26.49, rel=0.1)
        assert calculate_sub_index(18.5, breakpoints) == 49

    def test_pm25_moderate_range(self):
        """PM2.5 in moderate range (18.5-37.5 ug/m3) should give sub-index 50-100."""
        breakpoints = BREAKPOINTS["PM2.5"]
        # At exact boundary 18.5, matches first range (returns 49)
        assert calculate_sub_index(18.51, breakpoints) == pytest.approx(50, rel=0.1)
        assert calculate_sub_index(28, breakpoints) == pytest.approx(75, rel=0.1)
        assert calculate_sub_index(37.5, breakpoints) == 100

    def test_pm25_boundary_value_37_2(self):
        """PM2.5=37.2 should NOT fall in a gap (the bug we fixed)."""
        breakpoints = BREAKPOINTS["PM2.5"]
        result = calculate_sub_index(37.2, breakpoints)
        # 37.2 is in (18.5, 37.5, 50, 100) range
        # sub_idx = 50 + (100-50) * (37.2-18.5) / (37.5-18.5) = 50 + 50 * 18.7/19 ≈ 99.2
        assert result == pytest.approx(99.2, rel=0.1)
        assert result < 500  # Should NOT be max value (the bug)

    def test_pm25_above_max(self):
        """PM2.5 above max breakpoint should return max sub-index."""
        breakpoints = BREAKPOINTS["PM2.5"]
        assert calculate_sub_index(250, breakpoints) == 500

    def test_no2_ranges(self):
        """NO2 breakpoints should work correctly."""
        breakpoints = BREAKPOINTS["NO2"]
        assert calculate_sub_index(0, breakpoints) == 0
        assert calculate_sub_index(53, breakpoints) == 49  # End of first range
        assert calculate_sub_index(53.1, breakpoints) == pytest.approx(50, rel=0.1)  # Start of second
        assert calculate_sub_index(80, breakpoints) == pytest.approx(75.5, rel=0.1)


class TestCalculateAQI:
    """Tests for calculate_aqi function."""

    def test_good_air_quality(self):
        """Low pollutant levels should give high AQI (close to 100)."""
        pollutants = {"PM2.5": 10, "PM10": 30, "O3": 20}
        aqi = calculate_aqi(pollutants)
        assert aqi > 50  # Good air quality

    def test_moderate_air_quality(self):
        """Moderate pollutant levels should give AQI around 0-50."""
        pollutants = {"PM2.5": 30}  # Sub-index ~80
        aqi = calculate_aqi(pollutants)
        # AQI = 100 - 80 = 20
        assert 0 <= aqi <= 50

    def test_unhealthy_air_quality(self):
        """High pollutant levels should give negative AQI."""
        pollutants = {"PM2.5": 100}  # Very high
        aqi = calculate_aqi(pollutants)
        assert aqi < 0  # Unhealthy

    def test_worst_pollutant_determines_aqi(self):
        """AQI should be based on the worst pollutant."""
        # Good PM2.5, bad NO2
        pollutants = {"PM2.5": 5, "NO2": 150}  # NO2 in unhealthy range
        aqi = calculate_aqi(pollutants)
        # NO2=150 → sub-index ~125, AQI = 100 - 125 = -25
        assert aqi < 0

    def test_no_data_returns_default(self):
        """No pollutant data should return default AQI of 50."""
        assert calculate_aqi({}) == 50
        assert calculate_aqi({"UNKNOWN": 100}) == 50

    def test_negative_values_ignored(self):
        """Negative pollutant values should be ignored."""
        pollutants = {"PM2.5": -10, "PM10": 30}
        aqi = calculate_aqi(pollutants)
        # Only PM10 should be considered
        assert aqi > 0

    def test_bug_fix_pm25_37_2(self):
        """Regression test: PM2.5=37.2 should NOT cause AQI=-400."""
        pollutants = {"PM2.5": 37.2}
        aqi = calculate_aqi(pollutants)
        # 37.2 → sub-index ~99, AQI = 100 - 99 = 1
        assert aqi > -100  # Should NOT be -400
        assert -10 < aqi < 10  # Should be around 0


class TestGetAlertLevel:
    """Tests for get_alert_level function."""

    def test_good_level(self):
        assert get_alert_level(60) == "GOOD"
        assert get_alert_level(100) == "GOOD"

    def test_moderate_level(self):
        assert get_alert_level(50) == "MODERATE"
        assert get_alert_level(0) == "MODERATE"

    def test_low_level(self):
        assert get_alert_level(-50) == "LOW"
        assert get_alert_level(-100) == "LOW"

    def test_very_low_level(self):
        assert get_alert_level(-150) == "VERY_LOW"
        assert get_alert_level(-400) == "VERY_LOW"


class TestGetBenzeneLevel:
    """Tests for get_benzene_level function.

    Thresholds: GOOD: 1.0, MODERATE: 1.55, LOW: 2.10, VERY_LOW: 2.64
    """

    def test_no_benzene(self):
        assert get_benzene_level(0) is None
        assert get_benzene_level(0.5) is None
        assert get_benzene_level(0.99) is None

    def test_good_level(self):
        assert get_benzene_level(1.0) == "GOOD"
        assert get_benzene_level(1.2) == "GOOD"
        assert get_benzene_level(1.54) == "GOOD"

    def test_moderate_level(self):
        assert get_benzene_level(1.55) == "MODERATE"
        assert get_benzene_level(1.8) == "MODERATE"
        assert get_benzene_level(2.09) == "MODERATE"

    def test_low_level(self):
        assert get_benzene_level(2.10) == "LOW"
        assert get_benzene_level(2.4) == "LOW"
        assert get_benzene_level(2.63) == "LOW"

    def test_very_low_level(self):
        assert get_benzene_level(2.64) == "VERY_LOW"
        assert get_benzene_level(3.0) == "VERY_LOW"
        assert get_benzene_level(5.0) == "VERY_LOW"


class TestShouldAlert:
    """Tests for should_alert function."""

    def test_good_threshold(self):
        """GOOD threshold alerts when AQI drops to 50 or below."""
        assert should_alert(50, "GOOD") is True
        assert should_alert(51, "GOOD") is False
        assert should_alert(100, "GOOD") is False

    def test_moderate_threshold(self):
        """MODERATE threshold alerts when AQI drops below 0."""
        assert should_alert(-1, "MODERATE") is True
        assert should_alert(0, "MODERATE") is False
        assert should_alert(50, "MODERATE") is False

    def test_low_threshold(self):
        """LOW threshold alerts when AQI drops below -100."""
        assert should_alert(-101, "LOW") is True
        assert should_alert(-100, "LOW") is False
        assert should_alert(0, "LOW") is False

    def test_very_low_threshold(self):
        """VERY_LOW threshold alerts when AQI drops below -200."""
        assert should_alert(-201, "VERY_LOW") is True
        assert should_alert(-200, "VERY_LOW") is False
        assert should_alert(-100, "VERY_LOW") is False


class TestConfigLoaded:
    """Tests that config is loaded correctly."""

    def test_alert_levels_loaded(self):
        """ALERT_LEVELS should have all required keys."""
        assert "GOOD" in ALERT_LEVELS
        assert "MODERATE" in ALERT_LEVELS
        assert "LOW" in ALERT_LEVELS
        assert "VERY_LOW" in ALERT_LEVELS

    def test_benzene_thresholds_loaded(self):
        """BENZENE_THRESHOLDS should have all required keys."""
        assert "GOOD" in BENZENE_THRESHOLDS
        assert "MODERATE" in BENZENE_THRESHOLDS
        assert "LOW" in BENZENE_THRESHOLDS
        assert "VERY_LOW" in BENZENE_THRESHOLDS

    def test_breakpoints_loaded(self):
        """BREAKPOINTS should have all pollutant types."""
        expected_pollutants = ["PM2.5", "PM10", "O3", "NO2", "SO2", "CO", "NOX"]
        for pollutant in expected_pollutants:
            assert pollutant in BREAKPOINTS, f"Missing {pollutant} in BREAKPOINTS"
            assert len(BREAKPOINTS[pollutant]) == 6, f"{pollutant} should have 6 ranges"

    def test_breakpoints_are_continuous(self):
        """Each breakpoint range should be continuous (no gaps)."""
        for pollutant, ranges in BREAKPOINTS.items():
            for i in range(len(ranges) - 1):
                current_high = ranges[i][1]
                next_low = ranges[i + 1][0]
                assert current_high == next_low, \
                    f"{pollutant}: gap between {current_high} and {next_low}"


class TestReadingsCache:
    """Tests for Redis readings cache functions."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        mock = MagicMock()
        return mock

    @pytest.fixture
    def sample_reading(self):
        """Sample reading data for tests."""
        return {
            "aqi": 75,
            "level": "GOOD",
            "pollutants": {"PM2.5": 15.0, "PM10": 30.0},
            "pollutant_meta": {
                "PM2.5": {"alias": "PM2.5", "units": "µg/m³"},
                "PM10": {"alias": "PM10", "units": "µg/m³"},
            },
            "pm25": 15.0,
            "pm10": 30.0,
            "o3": 0,
            "no2": 0,
            "so2": 0,
            "co": 0,
            "benzene_ppb": 0,
            "benzene_level": None,
            "timestamp": "2025-12-25T10:00:00+02:00",
        }

    def test_cache_ttl_is_3_minutes(self):
        """Cache TTL should be 180 seconds (3 minutes)."""
        assert READINGS_CACHE_TTL == 180

    def test_get_cached_reading_hit(self, mock_redis, sample_reading):
        """get_cached_reading should return parsed JSON when cache hit."""
        station_id = 123
        mock_redis.get.return_value = json.dumps(sample_reading)

        with patch.object(check_alerts, 'get_redis', return_value=mock_redis):
            result = get_cached_reading(station_id)

        mock_redis.get.assert_called_once_with(f"reading:{station_id}")
        assert result == sample_reading
        assert result["aqi"] == 75
        assert result["pollutants"]["PM2.5"] == 15.0

    def test_get_cached_reading_miss(self, mock_redis):
        """get_cached_reading should return None when cache miss."""
        station_id = 456
        mock_redis.get.return_value = None

        with patch.object(check_alerts, 'get_redis', return_value=mock_redis):
            result = get_cached_reading(station_id)

        mock_redis.get.assert_called_once_with(f"reading:{station_id}")
        assert result is None

    def test_get_cached_reading_no_redis(self):
        """get_cached_reading should return None when Redis unavailable."""
        with patch.object(check_alerts, 'get_redis', return_value=None):
            result = get_cached_reading(123)

        assert result is None

    def test_set_cached_reading(self, mock_redis, sample_reading):
        """set_cached_reading should store JSON with correct TTL."""
        station_id = 789

        with patch.object(check_alerts, 'get_redis', return_value=mock_redis):
            set_cached_reading(station_id, sample_reading)

        mock_redis.setex.assert_called_once_with(
            f"reading:{station_id}",
            READINGS_CACHE_TTL,
            json.dumps(sample_reading)
        )

    def test_set_cached_reading_no_redis(self):
        """set_cached_reading should gracefully handle missing Redis."""
        with patch.object(check_alerts, 'get_redis', return_value=None):
            # Should not raise an exception
            set_cached_reading(123, {"aqi": 50})

    def test_cache_round_trip(self, mock_redis, sample_reading):
        """Data should survive a cache round-trip (set then get)."""
        station_id = 999
        stored_data = {}

        def mock_setex(key, ttl, value):
            stored_data[key] = value

        def mock_get(key):
            return stored_data.get(key)

        mock_redis.setex.side_effect = mock_setex
        mock_redis.get.side_effect = mock_get

        with patch.object(check_alerts, 'get_redis', return_value=mock_redis):
            # Set the cache
            set_cached_reading(station_id, sample_reading)

            # Get from cache
            result = get_cached_reading(station_id)

        assert result == sample_reading

    def test_cache_key_format(self, mock_redis):
        """Cache key should follow 'reading:{station_id}' format."""
        mock_redis.get.return_value = None  # Return None for cache miss

        with patch.object(check_alerts, 'get_redis', return_value=mock_redis):
            get_cached_reading(42)
            set_cached_reading(42, {"aqi": 50})

        # Verify key format in both operations
        mock_redis.get.assert_called_with("reading:42")
        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args[0]
        assert call_args[0] == "reading:42"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
