// Copyright (c) 2019 <Daisuke Aritomo>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// This file was taken from https://github.com/osyoyu/tantivy-tokenizer-tiny-segmenter
//
// For some reason tantivy doesn't see that the Tokenizer trait is fully implemented and
// can't convert the tokenizer into a BoxedTokenizer.
//
// More specifically the trait BoxableTokenizer has a higher kinded trait
// bound: A: for<'a> Tokenizer<'a> + Send + Sync. Compilation fails complaining
// that the trait bound isn't satisfied while it obviously is. Just copying the
// exactly same implementation seems to satisfy the compiler.
//
// Either I'm doing something wrong or the compiler is not smart enough.

use std::iter::Enumerate;
use tantivy::tokenizer::{Token, BoxTokenStream, TokenStream, Tokenizer};

#[derive(Debug, Clone, Default)]
pub struct TinySegmenterTokenizer;

impl TinySegmenterTokenizer {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Tokenizer for TinySegmenterTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        TinySegmenterTokenStream::new(text).into()
    }
}

pub struct TinySegmenterTokenStream {
    tinyseg_enum: Enumerate<std::vec::IntoIter<String>>,
    current_token: Token,
    offset_from: usize,
    offset_to: usize,
}

impl TinySegmenterTokenStream {
    pub fn new(text: &str) -> TinySegmenterTokenStream {
        TinySegmenterTokenStream {
            tinyseg_enum: tinysegmenter::tokenize(text).into_iter().enumerate(),
            current_token: Token::default(),
            offset_from: 0,
            offset_to: 0,
        }
    }
}

impl TokenStream for TinySegmenterTokenStream {
    fn advance(&mut self) -> bool {
        match self.tinyseg_enum.next() {
            Some((pos, term)) => {
                self.offset_from = self.offset_to;
                self.offset_to = self.offset_from + term.len();

                let offset_from = self.offset_from;
                let offset_to = self.offset_to;

                self.current_token = Token {
                    offset_from,
                    offset_to,
                    position: pos,
                    text: term,
                    position_length: 1,
                };

                true
            }

            None => false,
        }
    }

    fn token(&self) -> &Token {
        &self.current_token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.current_token
    }
}
