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
    (1, 26),   # Republic Day (January 26)
    (5, 1),    # Labour Day (May 1)
    (8, 15),   # Independence Day (August 15)
    (10, 2),   # Gandhi Jayanti (October 2)
    (12, 25),  # Christmas (December 25)
]

# Global variable to store loaded holidays
_LOADED_HOLIDAYS = None
_USING_RECURRING = False


def load_holidays(holiday_file=None):
    """
    Load NSE holidays from CSV file.

    Args:
        holiday_file (str or Path, optional): Path to CSV file with dates.
            Defaults to the standard NSE holidays file.

    Returns:
        set: Set of datetime.date objects representing holidays
    """
    global _LOADED_HOLIDAYS, _USING_RECURRING

    if holiday_file is None:
        holiday_file = DEFAULT_HOLIDAY_FILE

    holiday_file = Path(holiday_file)

    if not holiday_file.exists():
        # Fall back to recurring holidays
        _USING_RECURRING = True
        _LOADED_HOLIDAYS = RECURRING_HOLIDAYS
        return _LOADED_HOLIDAYS

    # Load holidays from CSV
    holidays = set()
    try:
        with open(holiday_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        date = datetime.strptime(line, '%Y-%m-%d').date()
                        holidays.add(date)
                    except ValueError:
                        continue  # Skip invalid date formats
        _LOADED_HOLIDAYS = holidays
        _USING_RECURRING = False
    except Exception:
        # Fall back to recurring holidays on any error
        _USING_RECURRING = True
        _LOADED_HOLIDAYS = RECURRING_HOLIDAYS

    return _LOADED_HOLIDAYS


def get_holidays():
    """
    Get the current holidays list/set.

    Returns:
        set or list: Set of date objects (if loaded from CSV) or
                     list of (month, day) tuples (if using recurring)
    """
    if _LOADED_HOLIDAYS is None:
        load_holidays()
    return _LOADED_HOLIDAYS


def is_public_holiday(date):
    """
    Check if a given date is a public holiday.

    Args:
        date: datetime.date or datetime.datetime object

    Returns:
        bool: True if the date is a public holiday, False otherwise
    """
    holidays = get_holidays()

    if _USING_RECURRING:
        # Check against (month, day) tuples
        return (date.month, date.day) in holidays
    else:
        # Check against actual date objects
        if hasattr(date, 'date'):
            date = date.date()
        
        # First check if it's in the CSV holidays
        if date in holidays:
            return True
        
        # Fallback to recurring holidays for dates not in CSV
        return (date.month, date.day) in RECURRING_HOLIDAYS


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


# For backward compatibility, expose as PUBLIC_HOLIDAYS
# This will be the loaded holidays set or recurring list
def _get_public_holidays():
    """Backward compatibility wrapper."""
    return get_holidays()


# This makes PUBLIC_HOLIDAYS behave like a constant but load holidays lazily
class _PublicHolidaysProxy:
    """Proxy object that loads holidays on first access."""

    def __contains__(self, item):
        holidays = get_holidays()
        if _USING_RECURRING:
            # item should be (month, day) tuple
            return item in holidays
        else:
            # item should be datetime.date
            return item in holidays

    def __iter__(self):
        return iter(get_holidays())

    def __len__(self):
        return len(get_holidays())

    def __repr__(self):
        return repr(get_holidays())


PUBLIC_HOLIDAYS = _PublicHolidaysProxy()

