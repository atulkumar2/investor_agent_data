"""
Indian Public Holidays Configuration

This module contains the list of Indian national public holidays that are
observed by NSE (National Stock Exchange of India). Markets are closed on
these dates.

Holidays are loaded from an external CSV file containing actual NSE holiday
dates, with fallback to a basic recurring holiday list if the file is not found.
"""

from datetime import datetime
from pathlib import Path

# Path to the comprehensive NSE holidays CSV file (relative to this module)
DEFAULT_HOLIDAY_FILE = Path(__file__).parent / "nse_holidays.csv"

# Fallback list of recurring Indian national public holidays (month, day)
# Used if the CSV file is not available
RECURRING_HOLIDAYS = [
    (1, 26),  # Republic Day (January 26)
    (5, 1),  # Labour Day (May 1)
    (8, 15),  # Independence Day (August 15)
    (10, 2),  # Gandhi Jayanti (October 2)
    (12, 25),  # Christmas (December 25)
]


class HolidayManager:
    """
    Manages loading and checking of NSE holidays.
    Encapsulates state to avoid global variable modification.
    """

    def __init__(self):
        self._loaded_holidays = None
        self._using_recurring = False

    def load_holidays(self, holiday_file=None):
        """
        Load NSE holidays from CSV file.

        Args:
            holiday_file (str or Path, optional): Path to CSV file with dates.
                Defaults to the standard NSE holidays file.

        Returns:
            set: Set of datetime.date objects representing holidays
        """
        if holiday_file is None:
            holiday_file = DEFAULT_HOLIDAY_FILE

        holiday_file = Path(holiday_file)

        if not holiday_file.exists():
            # Fall back to recurring holidays
            self._using_recurring = True
            self._loaded_holidays = RECURRING_HOLIDAYS
            return self._loaded_holidays

        # Load holidays from CSV
        holidays = set()
        try:
            with open(holiday_file, "r", encoding="utf-8") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if line:
                        try:
                            date = datetime.strptime(line, "%Y-%m-%d").date()
                            holidays.add(date)
                        except ValueError:
                            continue  # Skip invalid date formats
            self._loaded_holidays = holidays
            self._using_recurring = False
        except Exception:
            # Fall back to recurring holidays on any error
            self._using_recurring = True
            self._loaded_holidays = RECURRING_HOLIDAYS

        return self._loaded_holidays

    def get_holidays(self):
        """
        Get the current holidays list/set.

        Returns:
            set or list: Set of date objects (if loaded from CSV) or
                         list of (month, day) tuples (if using recurring)
        """
        if self._loaded_holidays is None:
            self.load_holidays()
        return self._loaded_holidays

    def is_public_holiday(self, date):
        """
        Check if a given date is a public holiday.

        Args:
            date: datetime.date or datetime.datetime object

        Returns:
            bool: True if the date is a public holiday, False otherwise
        """
        holidays = self.get_holidays()

        if self._using_recurring:
            # Check against (month, day) tuples
            return (date.month, date.day) in holidays

        # Check against actual date objects
        if hasattr(date, "date"):
            date = date.date()

        # First check if it's in the CSV holidays
        if date in holidays:
            return True

        # Fallback to recurring holidays for dates not in CSV
        return (date.month, date.day) in RECURRING_HOLIDAYS

    def __contains__(self, item):
        """Proxy method for 'in' operator check on the instance."""
        holidays = self.get_holidays()
        if self._using_recurring:
            # item should be (month, day) tuple
            return item in holidays
        else:
            # item should be datetime.date
            return item in holidays

    def __iter__(self):
        """Proxy method for iteration."""
        return iter(self.get_holidays())

    def __len__(self):
        """Proxy method for length."""
        return len(self.get_holidays())

    def __repr__(self):
        """Proxy method for representation."""
        return repr(self.get_holidays())


# Global singleton instance
_manager = HolidayManager()


def load_holidays(holiday_file=None):
    """
    Load NSE holidays from CSV file.
    Delegates to the singleton HolidayManager instance.
    """
    return _manager.load_holidays(holiday_file)


def get_holidays():
    """
    Get the current holidays list/set.
    Delegates to the singleton HolidayManager instance.
    """
    return _manager.get_holidays()


def is_public_holiday(date):
    """
    Check if a given date is a public holiday.
    Delegates to the singleton HolidayManager instance.
    """
    return _manager.is_public_holiday(date)


def get_holiday_name(month, day):
    """
    Get the name of the holiday for a given month and day.
    Only works for recurring holidays.

    Args:
        month (int): Month (1-12)
        day (int): Day of month

    Returns:
        str: Name of the holiday, or None if not a recurring holiday
    """
    holiday_names = {
        (1, 26): "Republic Day",
        (5, 1): "Labour Day",
        (8, 15): "Independence Day",
        (10, 2): "Gandhi Jayanti",
        (12, 25): "Christmas",
    }
    return holiday_names.get((month, day))


def _get_public_holidays():
    """Backward compatibility wrapper."""
    return get_holidays()


# Expose the singleton manager as PUBLIC_HOLIDAYS
# It behaves like a list/set due to magic methods
PUBLIC_HOLIDAYS = _manager
