#pragma once

#include <cstdarg>
#include <cstdio>

#define ZNS_DEBUG_LOG 1
#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_YELLOW "\x1b[33m"
#define ANSI_COLOR_BLUE "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN "\x1b[36m"
#define ANSI_COLOR_RESET "\x1b[0m"

#define ToKiB(num) (num) / (1024.0)
#define ToMiB(num) (num) / (1024.0 * 1024.0)
#define ToGiB(num) (num) / (1024.0 * 1024.0 * 1024.0)

enum Color {
  kDefault,
  kRed,
  kGreen,
  kYellow,
  kBlue,
  kMagenta,
  kCyan,
  kDisableLog
};

inline const char* ToString(Color color) {
  switch (color) {
    case kRed:
      return ANSI_COLOR_RED;
    case kGreen:
      return ANSI_COLOR_GREEN;
    case kYellow:
      return ANSI_COLOR_YELLOW;
    case kBlue:
      return ANSI_COLOR_BLUE;
    case kMagenta:
      return ANSI_COLOR_MAGENTA;
    case kCyan:
      return ANSI_COLOR_CYAN;
    default:
      return "";
  }
}

inline void ZnsLog(Color color, const char* fmt, ...) {
#ifdef ZNS_DEBUG_LOG
  if (color == kDisableLog) {
    return;
  }
  char buf[256];
  va_list vaList;
  va_start(vaList, fmt);
  vsprintf(buf, fmt, vaList);
  va_end(vaList);
  auto trailing = (color == kDefault) ? "" : ANSI_COLOR_RESET;
  printf("%s%s%s\n", ToString(color), buf, trailing);
#endif
}
