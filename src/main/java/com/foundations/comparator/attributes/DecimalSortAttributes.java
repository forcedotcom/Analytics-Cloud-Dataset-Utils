package com.foundations.comparator.attributes;

import java.math.RoundingMode;

public final class DecimalSortAttributes extends SortAttributes {
	
	public static final int DEFAULT_SCALE = 2;
	public static final RoundingMode DEFAULT_ROUNDING_MODE = RoundingMode.HALF_EVEN;

	private int _scale;
	private RoundingMode _roundingMode;
	
	public DecimalSortAttributes() {
		_scale = DEFAULT_SCALE;
		_roundingMode = DEFAULT_ROUNDING_MODE;
	}

	public void setScale(int value) {
		_scale = value;
	}

	public int getScale() {
		return _scale;
	}

	public void setRoundingMode(RoundingMode value) {
		_roundingMode = value;
	}

	public RoundingMode getRoundingMode() {
		return _roundingMode;
	}
 }
