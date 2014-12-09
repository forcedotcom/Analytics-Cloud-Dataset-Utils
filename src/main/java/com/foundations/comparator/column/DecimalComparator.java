package com.foundations.comparator.column;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.foundations.comparator.attributes.DecimalSortAttributes;

public final class DecimalComparator extends AbstractComparator {

	private int _scale;
	private RoundingMode _roundingMode;

	public DecimalComparator(String name, int sortOrder, DecimalSortAttributes attributes) {
		super(name, sortOrder, attributes);

		_scale = attributes.getScale();
		_roundingMode = attributes.getRoundingMode();
	}

	protected int extendedCompare(String a, String b) {
		BigDecimal aValue = new BigDecimal(a).setScale(_scale, _roundingMode);
		BigDecimal bValue = new BigDecimal(b).setScale(_scale, _roundingMode);

		return aValue.compareTo(bValue);
	}
}
