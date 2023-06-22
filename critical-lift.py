import math


def gas_compressibility_factor(
    pressure, temperature, spec_gravity, mol_frac_n2, mol_frac_co2, mol_frac_h2s
):
    """
    Calculates the gas compressibility factor (Z) using the Brill and Beggs correlation.

    Args:
        pressure (float): Instantaneous pressure in kPa.
        temperature (float): Instantaneous temperature in degC.
        spec_gravity (float): Specific gravity of the gas.
        mol_frac_n2 (float): Molar fraction of nitrogen (N2) in the gas.
        mol_frac_co2 (float): Molar fraction of carbon dioxide (CO2) in the gas.
        mol_frac_h2s (float): Molar fraction of hydrogen sulfide (H2S) in the gas.

    Returns:
        float: Gas compressibility factor (Z), unitless.
    """
    # pseudo-critical pressure, kPa
    pseudo_crit_press = (
        678
        - 50 * (spec_gravity - 0.5)
        - 206.7 * mol_frac_n2
        + 440 * mol_frac_co2
        + 606.7 * mol_frac_h2s
    ) * 6.894757

    # pseudo-critical temperature, K
    pseudo_crit_temp = (
        (
            326
            + 315.7 * (spec_gravity - 0.5)
            - 240 * mol_frac_n2
            - 83.3 * mol_frac_co2
            + 133.3 * mol_frac_h2s
        )
        * 5
        / 9
    )

    # pseudo-reduced pressure, unitless
    pseudo_red_press = pressure / pseudo_crit_press

    # pseudo-reduced temperature, unitless
    pseudo_red_temp = (temperature + 273.15) / pseudo_crit_temp

    # A parameter, unitless
    A = 1.39 * (pseudo_red_temp - 0.92) ** 0.5 - 0.36 * pseudo_red_temp - 0.101

    # B parameter, unitless
    B = (
        (0.62 - 0.23 * pseudo_red_temp) * pseudo_red_press
        + (0.066 / (pseudo_red_temp - 0.86) - 0.037) * pseudo_red_press**2
        + 0.32 * pseudo_red_press**6 / (10**A)
    )

    # C parameter, unitless
    C = 0.132 - 0.32 * math.log10(pseudo_red_temp)

    # D parameter, unitless
    D = 10 ** (0.3106 - 0.49 * pseudo_red_temp + 0.1824 * pseudo_red_temp**2)

    # compressibility factor, unitless
    Z = A + (1 - A) / math.exp(B) + C * pseudo_red_press**D

    return Z


def gas_density(spec_gravity, pressure, temperature, z):
    """
    Calculates the gas density in lb/ft3 using the ideal gas law.

    Args:
        spec_gravity (float): Specific gravity of the gas.
        pressure (float): Instantaneous pressure in kPa.
        temperature (float): Instantaneous temperature in degC.
        z (float): Instantaneous gas compressibility factor, unitless.

    Returns:
        float: Gas density in lb/ft3.
    """

    # convert pressure to psia
    pressure = pressure / 6.894757

    # convert temperature to degR
    temperature = (temperature + 273.15) * 9 / 5

    # gas density, lb/ft3
    gas_density = 2.699 * spec_gravity * pressure / (z * (temperature))

    return gas_density


def min_gas_velocity(gas_density):
    # gas density in lb/ft3

    # surface tension in dyne/cm (ref values: water = 60, condensate = 20)
    surface_tension = 60

    # liquid density in lb/ft3 (ref values: water = 62.416, condensate = 50)
    liquid_density = 62.416

    # minimum gas velocity in ft/s
    min_gas_vel = (
        1.593
        * surface_tension**0.25
        * (liquid_density - gas_density) ** 0.25
        / gas_density**0.5
    )

    return min_gas_vel


def critical_gas_rate(
    pressure, velocity, tubing_id, temperature, gas_compressibility_factor
):
    """
    Calculates the critical gas rate in E3m3/d using the Turner correlation.

    Args:
        pressure (float): Instantaneous pressure in kPa.
        velocity (float): Instantaneous velocity in ft/s.
        tubing_id (float): Tubing inner diameter in mm.
        temperature (float): Instantaneous temperature in degC.
        gas_compressibility_factor (float): Instantaneous gas compressibility factor, unitless.

    Returns: crit_gas_rate (float): critical gas rate in E3m3/d.

    """
    # convert pressure to psia
    pressure = pressure / 6.894757
    print("pressure = ", pressure)
    # convert temperature to degR
    temperature = (temperature + 273.15) * 9 / 5
    print("temperature = ", temperature)
    # calculate flow area of the tubing in ft2
    flow_area = (tubing_id * 0.00328084) ** 2 * math.pi / 4
    print("flow_area = ", flow_area)

    # critical gas rate in E3m3/d
    crit_gas_rate = (
        3.067
        * pressure
        * velocity
        * flow_area
        / (temperature * gas_compressibility_factor)
        * 28.317
    )

    return crit_gas_rate


if __name__ == "__main__":
    # test case

    # input parameters
    pressure = 700  # kPa
    temperature = 10  # degC
    tubing_id = 73  # mm
    spec_gravity = 0.70
    mol_frac_n2 = 0.1
    mol_frac_co2 = 0.2
    mol_frac_h2s = 0.0

    z = gas_compressibility_factor(
        pressure, temperature, spec_gravity, mol_frac_n2, mol_frac_co2, mol_frac_h2s
    )
    print("z = ", z)

    dg = gas_density(spec_gravity, pressure, temperature, z)

    vg = min_gas_velocity(dg)
    print("vg = ", vg)

    qg = critical_gas_rate(pressure, vg, tubing_id, temperature, z)
    print("qg = ", qg)
