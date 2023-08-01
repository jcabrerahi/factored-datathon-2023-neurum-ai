""" Modulo to test pure python functions."""
from src.utils.functions import CustomFunctions

def test_to_snakecase():
    """ test to_snakecase function."""
    # Test normal string
    input_str = "HelloWorld"
    expected_output = "hello_world"
    assert CustomFunctions.to_snakecase(input_str) == expected_output

    # Test string with spaces and special characters
    input_str = "   Hello   World!  "
    expected_output = "hello_world"
    assert CustomFunctions.to_snakecase(input_str) == expected_output

    # Test string with spaces and special characters
    input_str = "   estación__format_"
    expected_output = "estacion_format"
    assert CustomFunctions.to_snakecase(input_str) == expected_output

    # Test string with accented characters
    input_str = "áéíóúÁÉÍÓÚâêîôÂÊÎÔãõÃÕçÇ"
    expected_output = "aeiouaeiouaeioaeioaoaocc"
    assert CustomFunctions.to_snakecase(input_str) == expected_output

    # Test string with mixed case and dashes
    input_str = "Some-Text_withMixeD-cAsE"
    expected_output = "some_text_with_mixe_d_c_as_e"
    assert CustomFunctions.to_snakecase(input_str) == expected_output
