class CodeFormat:
    negation_word = 'not'

    @staticmethod
    def is_negative(code):
        return len(code) > 0 and code.startswith(CodeFormat.negation_word)

    @staticmethod
    def negative_to_simple(code):
        # For positive codes do nothing
        # For negative codes cut negation word + space + '[' symbol at the beginning of string and ']' at the end, then
        # split it by space. As the result list of simple codes.
        return code if not code.startswith(CodeFormat.negation_word) else code[len(CodeFormat.negation_word) + 2:-1].split()

    @staticmethod
    def simple_to_negative(codes):
        return f'{CodeFormat.negation_word} [{" ".join(codes)}]'
