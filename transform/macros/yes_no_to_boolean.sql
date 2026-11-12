{# NFZ flags are 'Y' / 'N'; anything else (empty string = not reported, NULL) stays unknown. #}
{% macro yes_no_to_boolean(column) -%}
    case {{ column }} when 'Y' then true when 'N' then false end
{%- endmacro %}
