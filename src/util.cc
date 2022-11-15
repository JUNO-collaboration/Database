#include "dbi/util.h"

namespace dbi {
    template<>
    void dbi_record_cnv<std::string>(const std::string& from, std::string& to) {
        to = from;
    }

}
